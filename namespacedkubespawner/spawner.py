"""
JupyterHub Spawner to spawn user notebooks on a Kubernetes cluster in per-
user namespaces.

This module exports `NamespacedKubeSpawner` class, which is the spawner
implementation that should be used by JupyterHub.
"""

import asyncio
import os
import sys

from tornado import gen
from tornado.ioloop import IOLoop

from jupyterhub.utils import exponential_backoff

from traitlets import Bool

from kubernetes.client.rest import ApiException
from kubernetes import client

from kubespawner import KubeSpawner
from kubespawner.clients import shared_client
from .reflector import MultiNamespaceResourceReflector


def rreplace(s, old, new, occurrence):
    """Convenience function from:
    https://stackoverflow.com/questions/2556108/\
    rreplace-how-to-replace-the-last-occurrence-of-an-expression-in-a-string
    """
    li = s.rsplit(old, occurrence)
    return new.join(li)


class PodReflector(MultiNamespaceResourceReflector):
    kind = 'pods'
    # FUTURE: These labels are the selection labels for the PodReflector. We
    # might want to support multiple deployments in the same namespace, so we
    # would need to select based on additional labels such as `app` and
    # `release`.
    labels = {
        'component': 'singleuser-server',
    }

    list_method_name = 'list_namespaced_pod'

    @property
    def pods(self):
        return self.resources


class EventReflector(MultiNamespaceResourceReflector):
    kind = 'events'

    list_method_name = 'list_namespaced_event'

    @property
    def events(self):
        return sorted(
            self.resources.values(),
            key=lambda x: x.last_timestamp,
        )


class MultiNamespacePodReflector(PodReflector):
    list_method_name = 'list_pod_for_all_namespaces'
    list_method_omit_namespace = True

    def _create_resource_key(self, resource):
        return (resource.metadata.namespace, resource.metadata.name)


class NamespacedKubeSpawner(KubeSpawner):
    """
    Implement a JupyterHub spawner to spawn pods in a Kubernetes Cluster with
    per-user namespaces.
    """

    _nfs_volumes = None
    rbacapi = None  # We need an RBAC client

    delete_namespace_on_stop = Bool(
        False,
        help="""
        If True, the entire namespace and its associated shadow PVs will
        be deleted when the lab pod stops.
        """
    )

    duplicate_nfs_pvs_to_namespace = Bool(
        False,
        help="""
        If true, NFS PVs in the JupyterHub namespace will be replicated
        to the user namespace.
        """
    )

    enable_namespace_quotas = Bool(
        False,
        help="""
        If True, will create a ResourceQuota object by calling
        `self.get_resource_quota_spec()` and create a quota with the resulting
        specification within the namespace.

        A subclass should override get_resource_quota_spec() to create a
        situationally-appropriate resource quota spec.
        """
    )

    def __init__(self, *args, **kwargs):
        _mock = kwargs.pop('_mock', False)
        super().__init__(*args, **kwargs)
        if _mock:
            # if testing, skip the rest of initialization
            # FIXME: rework initialization for easier mocking
            return

        selected_pod_reflector_classref = MultiNamespacePodReflector
        self.namespace = self.get_user_namespace()

        main_loop = IOLoop.current()

        def on_reflector_failure():
            self.log.critical("Pod reflector failed, halting Hub.")
            main_loop.stop()

        # Replace pod_reflector

        self.__class__.pod_reflector = selected_pod_reflector_classref(
            parent=self, namespace=self.namespace,
            on_failure=on_reflector_failure
        )
        self.log.debug("Created new pod reflector: " +
                       "%r" % self.__class__.pod_reflector)

        # Restart pod/event watcher

        self._start_watching_pods(replace=True)
        if self.events_enabled:
            self._start_watching_events(replace=True)

    def get_resource_quota_spec(self):
        """An implementation should override this by returning an appropriate
        kubernetes.client.V1ResourceQuotaSpec.
        """
        self.log.info("Dummy get_resource_quota_spec(); override for quotas")
        return None

    def get_user_namespace(self):
        """Return namespace for user pods (and ancillary objects)"""
        defname = self._namespace_default()
        # We concatenate the default namespace and the name so that we
        #  can continue having multiple Jupyter instances in the same
        #  k8s cluster in different namespaces.  The user namespaces must
        #  themselves be namespaced, as it were.
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

    @gen.coroutine
    def poll(self):
        """
        Check if the pod is still running.

        Uses the same interface as subprocess.Popen.poll(): if the pod is
        still running, returns None.  If the pod has exited, return the
        exit code if we can determine it, or 1 if it has exited but we
        don't know how.  These are the return values JupyterHub expects.

        Note that a clean exit will have an exit code of zero, so it is
        necessary to check that the returned value is None, rather than
        just Falsy, to determine that the pod is still running.
        """
        # have to wait for first load of data before we have a valid answer
        if not self.pod_reflector.first_load_future.done():
            yield self.pod_reflector.first_load_future
        data = self.pod_reflector.pods.get((self.namespace, self.pod_name),
                                           None)
        if data is not None:
            if data.status.phase == 'Pending':
                return None
            ctr_stat = data.status.container_statuses
            if ctr_stat is None:  # No status, no container (we hope)
                # This seems to happen when a pod is idle-culled.
                return 1
            for c in ctr_stat:
                # return exit code if notebook container has terminated
                if c.name == 'notebook':
                    if c.state.terminated:
                        # call self.stop to delete the pod
                        if self.delete_stopped_pods:
                            yield self.stop(now=True)
                        return c.state.terminated.exit_code
                    break
            # None means pod is running or starting up
            return None
        # pod doesn't exist or has been deleted
        return 1

    @gen.coroutine
    def _start(self):
        """Start the user's pod"""
        # Ensure namespace and necessary resources exist
        self._ensure_namespace()
        # record latest event so we don't include old
        # events from previous pods in self.events
        # track by order and name instead of uid
        # so we get events like deletion of a previously stale
        # pod if it's part of this spawn process
        events = self.events
        if events:
            self._last_event = events[-1].metadata.uid

        if self.storage_pvc_ensure:
            # Try and create the pvc. If it succeeds we are good. If
            # returns a 409 indicating it already exists we are good. If
            # it returns a 403, indicating potential quota issue we need
            # to see if pvc already exists before we decide to raise the
            # error for quota being exceeded. This is because quota is
            # checked before determining if the PVC needed to be
            # created.

            pvc = self.get_pvc_manifest()

            try:
                yield self.asynchronize(
                    self.api.create_namespaced_persistent_volume_claim,
                    namespace=self.namespace,
                    body=pvc
                )
            except ApiException as e:
                if e.status == 409:
                    self.log.info("PVC " + self.pvc_name +
                                  " already exists, so did not create" +
                                  " new pvc.")

                elif e.status == 403:
                    t, v, tb = sys.exc_info()

                    try:
                        yield self.asynchronize(
                            self.api.read_namespaced_persistent_volume_claim,
                            name=self.pvc_name,
                            namespace=self.namespace)
                    except ApiException:
                        raise v.with_traceback(tb)

                    self.log.info(
                        "PVC " + self.pvc_name + " already exists," +
                        " possibly have reached quota though.")

                else:
                    raise
        # If we run into a 409 Conflict error, it means a pod with the
        # same name already exists. We stop it, wait for it to stop, and
        # try again. We try 4 times, and if it still fails we give up.
        # FIXME: Have better / cleaner retry logic!
        retry_times = 4
        pod = yield self.get_pod_manifest()
        if self.modify_pod_hook:
            pod = yield gen.maybe_future(self.modify_pod_hook(self, pod))
        for i in range(retry_times):
            try:
                yield self.asynchronize(
                    self.api.create_namespaced_pod,
                    self.namespace,
                    pod,
                )
                break
            except ApiException as e:
                if e.status != 409:
                    # We only want to handle 409 conflict errors
                    self.log.exception("Failed for %s", pod.to_str())
                    raise
                self.log.info(
                    'Found existing pod %s, attempting to kill', self.pod_name)
                # TODO: this should show up in events
                yield self.stop(True)

                self.log.info(
                    'Killed pod %s, will try starting ' % self.pod_name +
                    'singleuser pod again')
        else:
            raise Exception(
                'Can not create user pod %s :' % self.pod_name +
                'already exists and could not be deleted')

        # we need a timeout here even though start itself has a timeout
        # in order for this coroutine to finish at some point.
        # using the same start_timeout here
        # essentially ensures that this timeout should never propagate up
        # because the handler will have stopped waiting after
        # start_timeout, starting from a slightly earlier point.
        try:
            yield exponential_backoff(
                lambda: self.is_pod_running(self.pod_reflector.pods.get(
                    (self.namespace, self.pod_name), None)),
                'pod/%s did not start in %s seconds!' % (
                    self.pod_name, self.start_timeout),
                timeout=self.start_timeout,
            )
        except TimeoutError:
            if self.pod_name not in self.pod_reflector.pods:
                # if pod never showed up at all,
                # restart the pod reflector which may have become disconnected.
                self.log.error(
                    "Pod %s never showed up in reflector;" % self.pod_name +
                    " restarting pod reflector."
                )
                self._start_watching_pods(replace=True)
            raise

        pod = self.pod_reflector.pods[(self.namespace, self.pod_name)]
        self.pod_id = pod.metadata.uid
        if self.event_reflector:
            self.log.debug(
                'pod %s events before launch: %s',
                self.pod_name,
                "\n".join(
                    [
                        "%s [%s] %s" % (event.last_timestamp,
                                        event.type, event.message)
                        for event in self.events
                    ]
                ),
            )
        return (pod.status.pod_ip, self.port)

    @gen.coroutine
    def stop(self, now=False):
        delete_options = client.V1DeleteOptions()

        if now:
            grace_seconds = 0
        else:
            # Give it some time, but not the default (which is 30s!)
            # FIXME: Move this into pod creation maybe?
            grace_seconds = 1

        delete_options.grace_period_seconds = grace_seconds
        self.log.info("Deleting pod %s", self.pod_name)
        try:
            yield self.asynchronize(
                self.api.delete_namespaced_pod,
                name=self.pod_name,
                namespace=self.namespace,
                body=delete_options,
                grace_period_seconds=grace_seconds,
            )
        except ApiException as e:
            if e.status == 404:
                self.log.warning(
                    "No pod %s to delete. Assuming already deleted.",
                    self.pod_name,
                )
            else:
                raise
        try:
            yield exponential_backoff(
                lambda: self.pod_reflector.pods.get((self.namespace,
                                                     self.pod_name), None) is
                None,
                'pod/%s did not disappear in %s seconds!' % (
                    self.pod_name, self.start_timeout),
                timeout=self.start_timeout,
            )
        except TimeoutError:
            self.log.error(
                "Pod %s did not disappear, " % self.pod_name +
                "restarting pod reflector")
            self._start_watching_pods(replace=True)
            raise
        if self.delete_namespace_on_stop:
            self.asynchronize(self._maybe_delete_namespace())

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
        if self.duplicate_nfs_pvs_to_namespace:
            self._replicate_nfs_pvs()
            self._create_pvcs_for_pvs()
        if self.service_account:
            self._ensure_namespaced_service_account()
        if self.enable_namespace_quotas:
            quota = self.get_resource_quota_spec()
            if quota:
                self._ensure_namespaced_resource_quota(quota)

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
        self.log.info("Clear to delete namespace '%s'" % namespace)
        # PVs are not really namespaced
        self._destroy_namespaced_pvs()
        self.log.info("Deleting namespace '%s'" % namespace)
        self.api.delete_namespace(namespace, client.V1DeleteOptions())
        return True

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
        mns = self._namespace_default()
        namespace = self.get_user_namespace()
        suffix = ""
        if mns:
            suffix = "-" + mns
        self._refresh_nfs_volumes(suffix=suffix)
        ns_suffix = "-" + namespace
        mtkey = "volume.beta.kubernetes.io/mount-options"
        for vol in self._nfs_volumes:
            pname = vol.metadata.name
            mtopts = vol.metadata.annotations.get(mtkey)
            if suffix:
                ns_name = rreplace(pname, suffix, ns_suffix, 1)
            else:
                ns_name = pname + "-" + namespace
            anno = {}
            if mtopts:
                anno[mtkey] = mtopts
            pv = client.V1PersistentVolume(
                spec=vol.spec,
                metadata=client.V1ObjectMeta(
                    annotations=anno,
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
        if (not namespace or namespace == self._namespace_default() or
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

    def _create_namespaced_account_objects(self):
        namespace = self.get_user_namespace()
        account = self.service_account
        if not account:
            self.log.info("No service account defined.")
            return (None, None, None)
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
        svcacct, role, rolebinding = self._create_namespaced_account_objects()
        if not svcacct:
            self.log.info("Service account not defined.")
            return
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

    def _ensure_namespaced_resource_quota(self, quotaspec):
        self.log.info("Entering ensure_namespaced_resource_quota()")
        namespace = self.get_user_namespace()
        qname = "quota-" + namespace
        quota = client.V1ResourceQuota(
            metadata=client.V1ObjectMeta(
                name=qname
            ),
            spec=quotaspec
        )
        self.log.info("Creating quota: %r" % quota)
        try:
            self.api.create_namespaced_resource_quota(namespace, quota)
        except ApiException as e:
            if e.status != 409:
                self.log.exception("Create resourcequota '%s'" % quota +
                                   "in namespace '%s' " % namespace +
                                   "failed: %s", str(e))
                raise
            else:
                self.log.info("Resourcequota '%r' " % quota +
                              "already exists in '%s'." % namespace)

    def _destroy_namespaced_resource_quota(self):
        # You don't usually have to call this, since it will get
        #  cleaned up as part of namespace deletion.
        namespace = self.get_user_namespace()
        qname = "quota-" + namespace
        dopts = client.V1DeleteOptions()
        self.log.info("Deleting resourcequota '%s'" % qname)
        self.api.delete_namespaced_resource_quota(qname, namespace, dopts)

    def _destroy_pvcs(self):
        # You don't usually have to call this, since it will get
        #  cleaned up as part of namespace deletion.
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

    def _delete_namespaced_service_account_objects(self):
        # You don't usually have to call this, since it will get
        #  cleaned up as part of namespace deletion.
        namespace = self.get_user_namespace()
        account = self.service_account
        if not account:
            self.log.info("Service account not defined.")
            return
        dopts = client.V1DeleteOptions()
        self.log.info("Deleting service accounts/role/rolebinding " +
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
