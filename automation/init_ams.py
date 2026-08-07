import logging

import requests
from argo_ams_library import (AmsServiceException, AmsUser, AmsUserProject,
                              ArgoMessagingService)

from argo_config import ArgoConfig

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 30


# use http request to create component user because ams library doesn't support it
def create_ams_component_account(
    ams_endpoint: str,
    ams_token: str,
    username: str,
    email: str,
    project: str,
    role: str,
    component: str,
    component_project: str,
):

    payload = {
        "email": email,
        "projects": [{"project": project, "roles": [role]}],
    }

    if component and component_project:
        payload["component"] = component
        payload["component_project"] = component_project

    url = f"https://{ams_endpoint}/v1/users/{username}"
    headers = {
        "x-api-key": ams_token,
        "Accept": "application/json",
    }
    try:
        response = requests.post(
            url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        logger.info(f"ams user: {username} created for project: {project}")

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            logger.warning(
                f"ams user: {username} for project: {project} already exists!"
            )
        else:
            raise


def init_ams(
    config: ArgoConfig,
    tenant_id: str,
    tenant_name: str,
) -> bool:
    """Initialise project, topic and subscriptions to ams"""
    # create admin client for ams to create new project
    ams = ArgoMessagingService(
        endpoint=config.ams_endpoint,
        token=config.ams_admin_token,
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
        endpoint=config.ams_endpoint, token=config.ams_admin_token, project=tenant_name
    )

    admin_username = f"{tenant_name}_admin"
    consumer_username = f"{tenant_name}_consumer"
    publisher_username = f"{tenant_name}_publisher"
    archiver_username = f"{tenant_name}_archiver"
    # map users and roles and component info, and create them
    user_roles = [
        (admin_username, "project_admin", None, None),
        (consumer_username, "consumer", "argo-engine", tenant_name),
        (publisher_username, "publisher", "argo-monbox", tenant_name),
        (archiver_username, "consumer", "argo-archiver", tenant_name),
    ]

    for username, role, component, component_admin in user_roles:
        try:
            if component and component_admin: 
                user = create_ams_component_account(
                    config.ams_endpoint,
                    config.ams_admin_token,
                    username,
                    config.argo_ops_email,
                    tenant_name,
                    role,
                    component,
                    component_admin,
                )

                user = ams.create_user(
                    AmsUser(
                        name=username,
                        projects=[AmsUserProject(project=tenant_name, roles=[role])],
                        email=config.argo_ops_email,
                    )
                )

                if user:
                    logger.info(f"ams project {tenant_name} - user created: {username}")
                    if role == "consumer" and username == consumer_username:
                        config.set_tenant_ams_access(tenant_id, tenant_name, user.token)

        except AmsServiceException as e:
            if e.code == 409:
                logger.warning(
                    f"ams project {tenant_name} - user {username} already exists"
                )
                if role == "consumer" and username == consumer_username:
                    user = ams.get_user(username)
                    if user:
                        config.set_tenant_ams_access(tenant_id, tenant_name, user.token)

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
