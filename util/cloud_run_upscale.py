import os

from dotenv import load_dotenv
from google.api_core.operation import Operation
from google.cloud import run_v2


def cloud_run_upscale(memory_limit: str | None = None, cpu_limit: str | None = None) -> int:
    """
    Upscales the specified Google Cloud Run service by updating its memory and/or CPU limits.

    This function updates the resource limits of a Cloud Run service based on the provided memory and CPU limits.
    It retrieves the service configuration from environment variables, and then applies the requested changes.
    At least one of 'memory_limit' or 'cpu_limit' must be provided to update the service.

    Args:
        memory_limit (str | None): The new memory limit for the Cloud Run service.
                                   This should be a string like '512Mi' or '1Gi'. If None, memory limit is not updated.
        cpu_limit (str | None): The new CPU limit for the Cloud Run service.
                                This should be a string representing the number of CPUs, e.g., '1', '2'. If None,
                                CPU limit is not updated.

    Raises:
        ValueError: If neither memory_limit nor cpu_limit is provided.

    Returns:
        int: The HTTP status code of the operation. 200 if successful, 500 otherwise.

    Note:
        The function expects the following environment variables to be set:
        - SERVICE_NAME: The name of the Cloud Run service to upscale.
        - SERVICE_REGION: The region where the Cloud Run service is deployed.
        - PROJECT_ID: The GCP project ID where the Cloud Run service resides.
    """
    if memory_limit is None and cpu_limit is None:
        raise ValueError("You must provide at least one of 'memory_limit' or 'cpu_limit' parameters.")

    load_dotenv(override=True)
    SERVICE_NAME: str = os.getenv("SERVICE_NAME", default="dvwa")
    SERVICE_REGION: str = os.getenv("SERVICE_REGION", default="us-central1")
    PROJECT_ID: str = os.getenv("PROJECT_ID", default="tsmccareerhack2024-icsd-grp1")

    client: run_v2.ServicesClient = run_v2.ServicesClient()
    resource_name: str = f"projects/{PROJECT_ID}/locations/{SERVICE_REGION}/services/{SERVICE_NAME}"

    service: run_v2.Service = client.get_service(name=resource_name)

    if memory_limit is not None:
        # Modify the memory limit
        service.template.containers[0].resources.limits["memory"] = memory_limit

    if cpu_limit is not None:
        # Modify the CPU limit
        service.template.containers[0].resources.limits["cpu"] = cpu_limit

    # Update the service
    operation: Operation = client.update_service(service=service)
    print(f"service: {resource_name}\nUpdating...")

    # Wait for the operation to complete
    response = operation.result()

    if response is not None:
        print(f"service: {response.name}\nUpdated!")
        return 200
    return 500


def get_resources_limits() -> dict:
    """
    Retrieves the current memory and CPU limits of a specified Google Cloud Run service.

    This function fetches the resource limits of a Cloud Run service. It uses environment variables to determine the
    service details and then queries the Cloud Run API to get the current memory and CPU limits.

    Returns:
        dict: A dictionary containing the memory and CPU limits. Example: {'memory': '512Mi', 'cpu': '1'}.

    Raises:
        RuntimeError: If there is an issue fetching the service details.

    Note:
        The function expects the following environment variables to be set:
        - SERVICE_NAME: The name of the Cloud Run service.
        - SERVICE_REGION: The region where the Cloud Run service is deployed.
        - PROJECT_ID: The GCP project ID where the Cloud Run service resides.
    """
    load_dotenv(override=True)
    SERVICE_NAME: str = os.getenv("SERVICE_NAME", default="default-service-name")
    SERVICE_REGION: str = os.getenv("SERVICE_REGION", default="default-region")
    PROJECT_ID: str = os.getenv("PROJECT_ID", default="default-project-id")

    client: run_v2.ServicesClient = run_v2.ServicesClient()
    resource_name: str = f"projects/{PROJECT_ID}/locations/{SERVICE_REGION}/services/{SERVICE_NAME}"

    try:
        service: run_v2.Service = client.get_service(name=resource_name)
        container = service.template.containers[0]

        memory_limit = container.resources.limits.get("memory", "Not specified")
        cpu_limit = container.resources.limits.get("cpu", "Not specified")

        return {"memory_limit": memory_limit, "cpu_limit": cpu_limit}

    except Exception as e:
        raise RuntimeError(f"Failed to fetch resource limits: {e}")


if __name__ == "__main__":
    cloud_run_upscale(memory_limit="512Mi", cpu_limit="1000m")
