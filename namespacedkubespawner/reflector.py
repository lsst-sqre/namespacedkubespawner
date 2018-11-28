# Inspired by, and based on, Adam Tilghman's Multi-Namespace work in
#  https://github.com/jupyterhub/kubespawner/pull/218
import time
from kubernetes import watch
from kubespawner.reflector import NamespacedResourceReflector
from traitlets import Bool
# This is kubernetes client implementation specific, but we need to know
# whether it was a network or watch timeout.
from urllib3.exceptions import ReadTimeoutError


class MultiNamespaceResourceReflector(NamespacedResourceReflector):
    list_method_omit_namespace = Bool(
        False,
        help="""
        If True, our calls to API `list_method_name` will omit a `namespace`
        argument.  Necessary for non-namespaced methods such as
        `list_pod_for_all_namespaces`
        """
    )

    def _create_resource_key(self, resource):
        """Maps a Kubernetes resource object onto a hashable Dict key;
        subclass may override if `resource.metadata.name` is not
        unique (e.g. pods across multiple namespaces)
        """
        return resource.metadata.name

    def _list_and_update(self):
        """
        Update current list of resources by doing a full fetch.
        Overwrites all current resource info.
        """

        list_args = {
            'label_selector': self.label_selector,
            'field_selector': self.field_selector,
            '_request_timeout': self.request_timeout
        }

        if not self.list_method_omit_namespace:
            list_args['namespace'] = self.namespace

        initial_resources = getattr(
            self.api, self.list_method_name)(**list_args)

        # This is an atomic operation on the dictionary!
        self.resources = {self._create_resource_key(
            p): p for p in initial_resources.items}
        # return the resource version so we can hook up a watch
        return initial_resources.metadata.resource_version

    def _watch_and_update(self):
        """
        Keeps the current list of resources up-to-date
        This method is to be run not on the main thread!
        We first fetch the list of current resources, and store that. Then we
        register to be notified of changes to those resources, and keep our
        local store up-to-date based on these notifications.
        We also perform exponential backoff, giving up after we hit 32s
        wait time. This should protect against network connections dropping
        and intermittent unavailability of the api-server. Every time we
        recover from an exception we also do a full fetch, to pick up
        changes that might've been missed in the time we were not doing
        a watch.
        Note that we're playing a bit with fire here, by updating a dictionary
        in this thread while it is probably being read in another thread
        without using locks! However, dictionary access itself is atomic,
        and as long as we don't try to mutate them (do a 'fetch / modify /
        update' cycle on them), we should be ok!
        """
        cur_delay = 0.1
        ns = self.namespace
        if self.list_method_omit_namespace:
            ns = "GLOBAL"
        self.log.info("watching for '%s' with " % self.kind +
                      "label selector '%s' " % self.label_selector +
                      "field selector '%s' " % self.field_selector +
                      "in namespace '%s'" % ns)
        while True:
            w = watch.Watch()
            try:
                resource_version = self._list_and_update()
                if not self.first_load_future.done():
                    # signal that we've loaded our initial data
                    self.first_load_future.set_result(None)
                watch_args = {
                    'label_selector': self.label_selector,
                    'field_selector': self.field_selector,
                    'resource_version': resource_version,
                }
                if not self.list_method_omit_namespace:
                    watch_args['namespace'] = self.namespace
                if self.request_timeout:
                    # set network receive timeout
                    watch_args['_request_timeout'] = self.request_timeout
                if self.timeout_seconds:
                    # set watch timeout
                    watch_args['timeout_seconds'] = self.timeout_seconds
                # in case of timeout_seconds, the w.stream just exits
                #  (no exception thrown)
                #     -> we stop the watcher and start a new one
                for ev in w.stream(
                        getattr(self.api, self.list_method_name),
                        **watch_args
                ):
                    cur_delay = 0.1
                    resource = ev['object']
                    if ev['type'] == 'DELETED':
                        # This is an atomic delete operation on the dictionary!
                        self.resources.pop(
                            self._create_resource_key(resource), None)
                    else:
                        # This is an atomic operation on the dictionary!
                        self.resources[self._create_resource_key(
                            resource)] = resource
                    if self._stop_event.is_set():
                        break
            except ReadTimeoutError:
                # network read time out, just continue and restart the watch
                continue
            except Exception:
                cur_delay = cur_delay * 2
                if cur_delay > 30:
                    self.log.exception(
                        "Watching resources never recovered, giving up")
                    if self.on_failure:
                        self.on_failure()
                    return
                self.log.exception(
                    "Error when watching resources, retrying in" +
                    " %ss" % cur_delay)
                time.sleep(cur_delay)
                continue
            finally:
                w.stop()
                if self._stop_event.is_set():
                    self.log.info("%s watcher stopped", self.kind)
                    break
