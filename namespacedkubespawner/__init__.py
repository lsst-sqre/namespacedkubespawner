"""
JupyterHub Spawner to spawn user notebooks on a Kubernetes cluster with per-
user namespaces.

After installation, you can enable it by adding::

    c.JupyterHub.spawner_class = 'namespacedkubespawner.NamespacedKubeSpawner'

in your `jupyterhub_config.py` file.
"""


from namespacedkubespawner.spawner import NamespacedKubeSpawner

__all__ = [NamespacedKubeSpawner]
