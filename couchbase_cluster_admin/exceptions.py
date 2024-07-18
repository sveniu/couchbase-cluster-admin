class AddToNotProvisionedNodeException(Exception):
    pass


class BackupPlanCreationException(Exception):
    pass


class BackupRepositoryCreationException(Exception):
    pass


class BucketCreationException(Exception):
    pass


class ScopeCreationException(Exception):
    pass


class CollectionCreationException(Exception):
    pass


class ClusterJoinException(Exception):
    pass


class ConnectToControllerOnJoinException(Exception):
    pass


class DeleteAlternateAddressException(Exception):
    pass


class IllegalArgumentError(ValueError):
    pass


class NodeRenameException(Exception):
    pass


class RebalanceException(Exception):
    pass


class SetAlternateAddressException(Exception):
    pass


class SetAuditSettingsException(Exception):
    pass


class SetAuthenticationException(Exception):
    pass


class SetClusterNameException(Exception):
    pass


class SetGsiSettingsException(Exception):
    pass


class SetMemoryQuotaException(Exception):
    pass


class UserCreationException(Exception):
    pass
