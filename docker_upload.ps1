$imageName = "islab-api"
$version = "latest"

Write-Output "Building image: ${imageName}:${version}"
docker build -t "${imageName}:${version}" .


Write-Output "Tagging image: ${imageName}:${version} and pushing to GCP"
docker tag "${imageName}:${version}" "us-central1-docker.pkg.dev/tsmccareerhack2024-icsd-grp1/tsmccareerhack2024-icsd-grp1-repository/${imageName}:${version}"
docker push "us-central1-docker.pkg.dev/tsmccareerhack2024-icsd-grp1/tsmccareerhack2024-icsd-grp1-repository/${imageName}:${version}"
