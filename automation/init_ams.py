import logging

from argo_ams_library import (AmsServiceException, AmsUser, AmsUserProject,
                              ArgoMessagingService)

logger = logging.getLogger(__name__)


def init_ams(
    ams_endpoint: str, ams_admin_token: str, tenant_name: str, argo_ops_email: str
) -> bool:
    """Initialise project, topic and subscriptions to ams"""

    # create admin client for ams to create new project
    ams = ArgoMessagingService(
        endpoint=ams_endpoint,
        token=ams_admin_token,
    )

    # Create project - skip if it exists
    try:
        ams.create_project(
            name=tenant_name, description=f"argo mon project for tenant {tenant_name}"
        )
        logger.info(f"Project created: {tenant_name}")
    except AmsServiceException as e:
        if e.code == 409:
            logger.warning(f"project {tenant_name} already exists")
        else:
            raise

    # Recreate the ams client for using the specific project (ams library necessity to select new project)

    ams = ArgoMessagingService(
        endpoint=ams_endpoint, token=ams_admin_token, project=tenant_name
    )

    admin_username = f"{tenant_name}_admin"
    consumer_username = f"{tenant_name}_consumer"
    publisher_username = f"{tenant_name}_publisher"
    archiver_username = f"{tenant_name}_archiver"
    # map users and roles and create them
    user_roles = [
        (admin_username, "project_admin"),
        (consumer_username, "consumer"),
        (publisher_username, "publisher"),
        (archiver_username, "publisher"),
    ]

    for username, role in user_roles:
        try:
            user = ams.create_user(
                AmsUser(
                    name=username,
                    projects=[AmsUserProject(project=tenant_name, roles=[role])],
                    email=argo_ops_email,
                )
            )

            if user:
                logger.info(f"ams project {tenant_name} - user created: {username}")
        except AmsServiceException as e:
            if e.code == 409:
                logger.warning(
                    f"ams project {tenant_name} - user {username} already exists"
                )
            else:
                logger.error(
                    f"ams project {tenant_name} - could not set up user: {username}"
                )
                return False

    # get topic metric data or create if if it doesn't exist
    if not ams.has_topic("metric_data"):
        ams.create_topic("metric_data")
    topic_metric_data = ams.get_topic("metric_data")

    if topic_metric_data:
        ams.modifyacl_topic("metric_data", [publisher_username])
        logger.info(f"ams project {tenant_name} - topic created: metric_data")
    else:
        logger.info(f"ams project {tenant_name} - could not set up topic: metric_data")
        return False

    # set up subscriptions
    subs = [
        ("ingest_metric", consumer_username),
        ("status_metric", consumer_username),
        ("archive_metric", archiver_username),
    ]

    for sub_name, sub_username in subs:
        if not ams.has_sub(sub_name):
            ams.create_sub(sub_name, "metric_data")
        sub = ams.get_sub(sub_name)
        if sub:
            ams.modifyacl_sub(sub_name, [sub_username])
            logger.info(f"ams project {tenant_name} - subscription created: {sub_name}")
        else:
            logger.error(
                f"ams project {tenant_name} - could not set up subscription: {sub_name}"
            )
            return False

    return True
