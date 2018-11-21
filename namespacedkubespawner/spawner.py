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


def rreplace(s, old, new, occurrence):
    """Convenience function from:
    https://stackoverflow.com/questions/2556108/\
    rreplace-how-to-replace-the-last-occurrence-of-an-expression-in-a-string
    """
    li = s.rsplit(old, occurrence)
    return new.join(li)


class NamespacedKubeSpawner(KubeSpawner):
    """
    Implement a JupyterHub spawner to spawn pods in a Kubernetes Cluster with
    per-user namespaces.
    """

    _nfs_volumes = None
    _mynamespace = None
    rbacapi = None  # We need an RBAC client
    # Reflectors now have user namespaces built into their names

    @property
    def pod_reflector(self):
        """alias to reflectors[namespace + '-pods']"""
        return self.reflectors[self.get_user_namespace() + '-pods']

    @property
    def event_reflector(self):
        """alias to reflectors[namespace + '-events']"""
        if self.events_enabled:
            return self.reflectors[self.get_user_namespace() + '-events']

    def __init__(self, *args, **kwargs):
        _mock = kwargs.pop('_mock', False)
        super().__init__(*args, **kwargs)
        self._refresh_mynamespace()
        self.namespace = self.get_user_namespace()

        if not _mock:
            self._ensure_namespace()
            self._start_watching_pods()  # Need to do it once per user
            if self.events_enabled:
                self._start_watching_events()

    def get_user_namespace(self):
        """Return namespace for user pods (and ancillary objects)"""
        defname = self._namespace_default()
        # We concatenate the current namespace and the name so that we
        #  can continue having multiple Jupyter instances in the same
        #  k8s cluster in different namespaces.  The user namespaces must
        #  themselves be namespaced, as it were.
        defname = self._mynamespace
        if self.user and self.user.name:
            uname = self.user.name
            return defname + "-" + uname
        return defname

    def _refresh_mynamespace(self):
        self._mynamespace = self._get_mynamespace()

    def _get_mynamespace(self):
        ns_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
        if os.path.exists(ns_path):
            with open(ns_path) as f:
                return f.read().strip()
        return None

    def _start_watching_events(self, replace=False):
        """Start the events reflector

        If replace=False and the event reflector is already running,
        do nothing.

        If replace=True, a running pod reflector will be stopped
        and a new one started (for recovering from possible errors).

        Note that the namespace becomes part of the reflector name.
        """
        return self._start_reflector(
            self.get_user_namespace() + "-events",
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
        return self._start_reflector(self.get_user_namespace() + "-pods",
                                     PodReflector, replace=replace)

    def start(self):
        """Start the user's pod"""
        return super().start()

    @gen.coroutine
    def stop(self, now=False):
        """Stop the user's pod, and try to delete ancillary objects and
        finally the namespace"""
        rc = super().stop()
        delled = self._maybe_delete_namespace()
        if not delled:
            self.asynchronize(self._async_delete_namespace())
        return rc

    def _ensure_namespace(self):
        """Here we make sure that the namespace exists, creating it if
        it does not.  That requires a ClusterRole that can list and create
        namespaces.

        We clone the (static) NFS PVs and then attach namespaced PVCs to them.
        Thus the role needs to be able to list and create PVs and PVCs.

        If we create the namespace, we also create (if needed) a ServiceAccount
        within it to allow the user pod to spawn dask pods."""
        self.log.info("Entered _ensure_namespace()")
        namespace = self.get_user_namespace()
        self.log.info("_ensure_namespace(): namespace '%s'" % namespace)
        ns = client.V1Namespace(
            metadata=client.V1ObjectMeta(name=namespace))
        try:
            self.log.info("Creating namespace '%s'" % namespace)
            self.api.create_namespace(ns)
        except ApiException as e:
            if e.status != 409:
                estr = "Create namespace '%s' failed: %s" % (ns, str(e))
                self.log.exception(estr)
                raise
            else:
                self.log.info("Namespace '%s' already exists." % namespace)
        self._replicate_nfs_pvs()
        self._create_pvcs_for_pvs()
        if self.service_account:
            self._ensure_namespaced_service_account()

    async def _async_delete_namespace(self, delay=75):
        namespace = self.get_user_namespace()
        self.log.info("Waiting %d seconds " % delay +
                      "for pods in namespace '%s' to exit." % namespace)
        await asyncio.sleep(delay)
        await self.asynchronize(self._maybe_delete_namespace())

    def _maybe_delete_namespace(self):
        """Here we try to delete the namespace.  If it has no running pods,
        we can delete it, as well as our shadow PVs we created.

        This requires a cluster role that can delete namespaces and PVs."""
        namespace = self.get_user_namespace()
        podlist = self.api.list_namespaced_pod(namespace)
        clear_to_delete = True
        if podlist and podlist.items and len(podlist.items) > 0:
            clear_to_delete = self._check_pods(podlist.items)
        if not clear_to_delete:
            self.log.info("Not deleting namespace '%s'" % namespace)
            return False
        # These destructors are optional, because the namespace will
        #  take care of it.
        self.log.info("Clear to delete namespace '%s'" % namespace)
        self._destroy_pvcs()
        if self.service_account:
            self.log.info("Deleting service " +
                          "account '%s'" % self.service_account)
            self._delete_namespaced_service_account()
        # But not this next one, because PVs are not really namespaced
        self._destroy_namespaced_pvs()
        self.log.info("Deleting namespace '%s'" % namespace)
        self.api.delete_namespace(namespace, client.V1DeleteOptions())
        return True

    def _destroy_pvcs(self):
        namespace = self.get_user_namespace()
        pvclist = self.api.list_namespaced_persistent_volume_claim(namespace)
        if pvclist and pvclist.items and len(pvclist.items) > 0:
            dopts = client.V1DeleteOptions()
            for pvc in pvclist.items:
                name = pvc.metadata.name
                self.log.info("Deleting PVC '%s' " % name +
                              "from namespace '%s'" % namespace)
                self.api.delete_namespaced_persistent_volume_claim(name,
                                                                   namespace,
                                                                   dopts)

    def _get_nfs_volumes(self, suffix=""):
        # This may be LSST-specific.  We're building a list of all NFS-
        #  mounted PVs, so we can later create namespaced PVCs for each of
        #  them.
        #
        # Suffix allows us to only categorize the PVs of a particular form;
        #  see the comment on _replicate_nfs_pvs() for the rationale.
        self.log.info("Refreshing NFS volume list")
        pvlist = self.api.list_persistent_volume()
        vols = []
        if pvlist and pvlist.items and len(pvlist.items) > 0:
            for pv in pvlist.items:
                if (pv and pv.spec and hasattr(pv.spec, "nfs") and
                        pv.spec.nfs):
                    if suffix:
                        if not pv.metadata.name.endswith(suffix):
                            continue
                    vols.append(pv)
                    self.log.debug("Found NFS volume '%s'" % pv.metadata.name)
        return vols

    def _refresh_nfs_volumes(self, suffix=""):
        vols = self._get_nfs_volumes(suffix)
        self._nfs_volumes = vols

    def _replicate_nfs_pvs(self):
        # A Note on Namespaces
        #
        # PersistentVolumes binds are exclusive,
        #  and since PersistentVolumeClaims are namespaced objects,
        #  mounting claims with “Many” modes (ROX, RWX) is only
        #  possible within one namespace.
        #
        # (https://kubernetes.io/docs/concepts/storage/persistent-volumes)
        #
        # The way we do this at LSST is that an NFS PV is statically created
        #  with the name suffixed with the same namespace as the Hub
        #  (e.g. "jld-fileserver-projects-jupyterlabdemo")
        #
        # Then when a new user namespace is created, we duplicate the NFS
        #  PV to one with the new namespace appended
        #  (e.g. "jld-fileserver-projects-jupyterlabdemo-athornton")
        #
        # Then we can bind PVCs to the new (effectively, namespaced) PVs
        #  and everything works.
        self.log.info("Replicating NFS PVs")
        mns = self._get_mynamespace()
        namespace = self.get_user_namespace()
        suffix = ""
        if mns:
            suffix = "-" + mns
        self._refresh_nfs_volumes(suffix=suffix)
        ns_suffix = "-" + namespace
        for vol in self._nfs_volumes:
            pname = vol.metadata.name
            if suffix:
                ns_name = rreplace(pname, suffix, ns_suffix, 1)
            else:
                ns_name = pname + "-" + namespace
            pv = client.V1PersistentVolume(
                spec=vol.spec,
                metadata=client.V1ObjectMeta(
                    name=ns_name,
                    labels={"name": ns_name}
                )
            )
            # It is new, therefore unclaimed.
            pv.spec.claim_ref = None
            try:
                self.api.create_persistent_volume(pv)
            except ApiException as e:
                if e.status != 409:
                    self.log.exception("Create PV '%s' " % ns_name +
                                       "failed: %s" % str(e))
                raise
            else:
                self.log.info("PV '%s' already exists." % ns_name)

    def _destroy_namespaced_pvs(self):
        namespace = self.get_user_namespace()
        if (not namespace or namespace == self._mynamespace or
                namespace == "default"):
            self.log.error("Will not destroy PVs for " +
                           "namespace '%r'" % namespace)
            return
        vols = self._get_nfs_volumes(suffix="-" + namespace)
        dopts = client.V1DeleteOptions()
        for v in vols:
            self.api.delete_persistent_volume(v.metadata.name, dopts)

    def _create_pvcs_for_pvs(self):
        self.log.info("Creating PVCs for PVs.")
        namespace = self.get_user_namespace()
        suffix = "-" + namespace
        vols = self._get_nfs_volumes(suffix=suffix)
        for vol in vols:
            name = vol.metadata.name
            pvcname = rreplace(name, suffix, "", 1)
            pvd = client.V1PersistentVolumeClaim(
                spec=client.V1PersistentVolumeClaimSpec(
                    volume_name=name,
                    access_modes=vol.spec.access_modes,
                    resources=client.V1ResourceRequirements(
                        requests=vol.spec.capacity
                    ),
                    selector=client.V1LabelSelector(
                        match_labels={"name": name}
                    ),
                    storage_class_name=vol.spec.storage_class_name
                )
            )
            md = client.V1ObjectMeta(name=pvcname,
                                     labels={"name": pvcname})
            pvd.metadata = md
            self.log.info("Creating PVC '%s' in namespace '%s'" % (pvcname,
                                                                   namespace))
            try:
                self.api.create_namespaced_persistent_volume_claim(namespace,
                                                                   pvd)
            except ApiException as e:
                if e.status != 409:
                    self.log.exception("Create PVC '%s' " % pvcname +
                                       "in namespace '%s' " % namespace +
                                       "failed: %s" % str(e))
                    raise
                else:
                    self.log.info("PVC '%s' " % pvcname +
                                  "in namespace '%s' " % namespace +
                                  "already exists.")

    def _check_pods(self, items):
        namespace = self.get_user_namespace()
        for i in items:
            if i and i.status:
                phase = i.status.phase
                if (phase is "Running" or phase is "Unknown"
                        or phase is "Pending"):
                    self.log.info("Pod in state '%s'; " % phase +
                                  "cannot delete namespace '%s'." % namespace)
                    return False
        return True

    def _make_account_objects(self):
        namespace = self.get_user_namespace()
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
            subjects=[client.V1Subject(
                kind="ServiceAccount",
                name=account,
                namespace=namespace)]
        )
        return svcacct, role, rolebinding

    def _ensure_namespaced_service_account(self):
        """Create a service account with role and rolebinding to allow it
        to manipulate pods in the namespace."""
        namespace = self.get_user_namespace()
        account = self.service_account
        svcacct, role, rolebinding = self._make_account_objects()
        try:
            self.api.create_namespaced_service_account(
                namespace=namespace,
                body=svcacct)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create service account '%s' " % account +
                                   "in namespace '%s' " % namespace +
                                   "failed: %s" % str(e))
                raise
            else:
                self.log.info("Service account '%s' " % account +
                              "in namespace '%s' already exists." % namespace)
        if not self.rbacapi:
            self.log.info("Creating RBAC API Client.")
            self.rbacapi = shared_client('RbacAuthorizationV1Api')
        try:
            self.rbacapi.create_namespaced_role(
                namespace,
                role)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create role '%s' " % account +
                                   "in namespace '%s' " % namespace +
                                   "failed: %s" % str(e))
                raise
            else:
                self.log.info("Role '%s' " % account +
                              "already exists in namespace '%s'." % namespace)
        try:
            self.rbacapi.create_namespaced_role_binding(
                namespace,
                rolebinding)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create rolebinding '%s'" % account +
                                   "in namespace '%s' " % namespace +
                                   "failed: %s", str(e))
                raise
            else:
                self.log.info("Rolebinding '%s' " % account +
                              "already exists in '%s'." % namespace)

    def _delete_namespaced_service_account(self):
        namespace = self.get_user_namespace()
        account = self.service_account
        dopts = client.V1DeleteOptions()
        self.log_info("Deleting service accounts/role/rolebinding " +
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
