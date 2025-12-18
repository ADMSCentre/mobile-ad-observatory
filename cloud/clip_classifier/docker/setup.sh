aws ecr create-repository \
  --repository-name moat_clip_classifier_docker_ecr \
  --image-scanning-configuration scanOnPush=true \
  --region ap-southeast-2
