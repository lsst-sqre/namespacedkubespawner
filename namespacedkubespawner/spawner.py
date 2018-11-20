"""
JupyterHub Spawner to spawn user notebooks on a Kubernetes cluster in per-
user namespaces.

This module exports `NamespacedKubeSpawner` class, which is the actual spawner
implementation that should be used by JupyterHub.
"""

import asyncio
import os

from tornado import gen
from traitlets import default

from kubernetes.client.rest import ApiException
from kubernetes import client

from kubespawner import KubeSpawner
from kubespawner.clients import shared_client
from kubespawner.spawner import PodReflector, EventReflector


class NamespacedKubeSpawner(KubeSpawner):
    """
    Implement a JupyterHub spawner to spawn pods in a Kubernetes Cluster with
    per-user namespaces.
    """

    rbacapi = None  # We need an RBAC client
    # Reflectors now have namespaces built into their names

    @property
    def pod_reflector(self):
        """alias to reflectors[namespace + '-pods']"""
        return self.reflectors[self._namespace_default() + '-pods']

    @property
    def event_reflector(self):
        """alias to reflectors[namespace + '-events']"""
        if self.events_enabled:
            return self.reflectors[self._namespace_default() + '-events']

    def __init__(self, *args, **kwargs):
        _mock = kwargs.pop('_mock', False)
        super().__init__(*args, **kwargs)
        self.namespace = self._namespace_default()
        if not _mock:
            self._start_watching_pods()  # Need to do it once per user
            if self.events_enabled:
                self._start_watching_events()

    @default('namespace')
    def _namespace_default(self):
        """
        Set namespace default to user name if we have one, otherwise
        set namespace default to current namespace if running in a k8s cluster

        If not in a k8s cluster with service accounts enabled, default to
        `default`
        """
        if self.user and self.user.name:
            return self.user.name
        ns_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
        if os.path.exists(ns_path):
            with open(ns_path) as f:
                return f.read().strip()
        return 'default'

    def _start_watching_events(self, replace=False):
        """Start the events reflector

        If replace=False and the event reflector is already running,
        do nothing.

        If replace=True, a running pod reflector will be stopped
        and a new one started (for recovering from possible errors).

        Note that the namespace becomes part of the reflector name.
        """
        self._ensure_namespace()
        return self._start_reflector(
            self._namespace_default() + "-events",
            EventReflector,
            fields={"involvedObject.kind": "Pod"},
            replace=replace,
        )

    def _start_watching_pods(self, replace=False):
        """Start the pod reflector

        If replace=False and the pod reflector is already running,
        do nothing.

        If replace=True, a running pod reflector will be stopped
        and a new one started (for recovering from possible errors).

        Note that the namespace becomes part of the reflector name.
        """
        self._ensure_namespace()
        return self._start_reflector(self._namespace_default() + "-pods",
                                     PodReflector, replace=replace)

    def start(self):
        """Start the user's pod"""
        self._ensure_namespace()
        return super().start()

    @gen.coroutine
    def stop(self, now=False):
        rc = super().stop()
        delled = self._maybe_delete_namespace()
        if not delled:
            self.asynchronize(self._async_delete_namespace())
        return rc

    def _ensure_namespace(self):
        """And here we make sure that the namespace exists, creating it if
        it does not.  That requires a ClusterRole that can list and create
        namespaces.

        If we create the namespace, we also create (if needed) a ServiceAccount
        within it to allow the user pod to spawn dask pods."""
        self.log.info("_ensure_namespace()")
        namespace = self._namespace_default()
        self.log.info("_ensure_namespace(): namespace '%s'" % namespace)
        ns = client.V1Namespace(
            metadata=client.V1ObjectMeta(name=namespace))
        try:
            self.log.info("Creating namespace '%s'" % namespace)
            yield self.asynchronize(
                self.api.create_namespace,
                ns)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create namespace '%s' " % namespace,
                                   "failed: %s", str(e))
                raise
            else:
                self.log.info("Namespace '%s' already exists." % namespace)
        if self.service_account:
            self._ensure_namespaced_service_account()

    async def _async_delete_namespace(self, delay=75):
        namespace = self._namespace_default()
        self.log.info("Waiting %d seconds " % delay,
                      "for pods in namespace '%s' to exit." % namespace)
        await asyncio.sleep(delay)
        await self.asynchronize(self._maybe_delete_namespace())

    def _maybe_delete_namespace(self):
        """Here we try to delete the namespace.  If it has no running pods,
        we can delete it and its associated ServiceAccount, if any."""
        namespace = self._namespace_default()
        podlist = self.api.list_namespaced_pod(namespace)
        clear_to_delete = True
        if podlist and podlist.items and len(podlist.items) > 0:
            clear_to_delete = self._check_pods(podlist.items)
        if not clear_to_delete:
            self.log.info("Not deleting namespace '%s'" % namespace)
            return False
        if self.service_account:
            self.log.info("Deleting service ",
                          "account '%s'" % self.service_account)
            self._delete_namespaced_service_account()
        self.log.info("Deleting namespace '%s'" % namespace)
        self.api.delete_namespace(namespace, client.V1DeleteOptions())
        return True

    def _check_pods(self, items):
        namespace = self._namespace_default()
        for i in items:
            if i and i.status:
                phase = i.status.phase
                if (phase is "Running" or phase is "Unknown"
                        or phase is "Pending"):
                    self.log.info("Pod in state '%s'; " % phase,
                                  "cannot delete namespace '%s'." % namespace)
                    return False
        return True

    def _make_account_objects(self):
        namespace = self._namespace_default()
        account = self.service_account
        md = client.V1ObjectMeta(name=account)
        svcacct = client.V1ServiceAccount(metadata=md)
        rules = [
            client.V1PolicyRule(
                api_groups=[""],
                resources=["pods"],
                verbs=["list", "create", "delete"]
            )
        ]
        role = client.V1Role(
            rules=rules,
            metadata=md)
        rolebinding = client.V1RoleBinding(
            metadata=md,
            role_ref=client.V1RoleRef(api_group="rbac.authorization.k8s.io",
                                      kind="Role",
                                      name=account),
            subjects=client.V1Subject(
                kind="ServiceAccount",
                name=account,
                namespace=namespace)
        )
        return svcacct, role, rolebinding

    def _ensure_namespaced_service_account(self):
        """Create a service account with role and rolebinding to allow it
        to manipulate pods in the namespace."""
        namespace = self._namespace_default()
        account = self.service_account
        svcacct, role, rolebinding = self._make_account_objects()
        try:
            self.api.create_namespaced_service_account(
                namespace=namespace,
                body=svcacct)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create service account '%s' " % account,
                                   "in namespace '%s' " % namespace,
                                   "failed: %s" % str(e))
                raise
            else:
                self.log.info("Service account '%s' " % account,
                              "'%s' already exists." % namespace)
        if not self.rbacapi:
            self.rbacapi = shared_client(client.RbacAuthorizationV1Api)
        try:
            self.rbacapi.create_namespaced_role(
                namespace,
                role)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create role '%s' " % account,
                                   "in namespace '%s' " % namespace,
                                   "failed: %s" % str(e))
                raise
            else:
                self.log.info("Role '%s' " % account,
                              "already exists in '%s'." % namespace)
        try:
            self.rbacapi.create_namespaced_role_binding(
                namespace,
                rolebinding)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create rolebinding '%s'" % account,
                                   "in namespace '%s' " % namespace,
                                   "failed: %s", str(e))
                raise
            else:
                self.log.info("Rolebinding '%s' " % account,
                              "already exists in '%s'." % namespace)

    def _delete_namespaced_service_account(self):
        namespace = self._namespace_default()
        account = self.service_account
        dopts = client.V1DeleteOptions()
        self.log_info("Deleting service accounts/role/rolebinding ",
                      "for %s" % namespace)
        self.rbacapi.delete_namespaced_role_binding(
            account,
            namespace,
            dopts)
        self.rbacapi.delete_namespaced_role(
            account,
            namespace,
            dopts)
        self.api.delete_namespaced_service_account(
            account,
            namespace,
            dopts)
