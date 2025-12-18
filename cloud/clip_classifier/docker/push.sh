aws ecr get-login-password --region ap-southeast-2 | docker login --username AWS --password-stdin 519969025508.dkr.ecr.ap-southeast-2.amazonaws.com

if [ "$1" == "--rebuild" ]; then
	echo "Running rebuild..."
	docker buildx build --platform linux/amd64 --pull --no-cache -t 519969025508.dkr.ecr.ap-southeast-2.amazonaws.com/moat_clip_classifier_docker_ecr:latest .
else
	echo "Running normal build..."
	docker buildx build --platform linux/amd64 -t 519969025508.dkr.ecr.ap-southeast-2.amazonaws.com/moat_clip_classifier_docker_ecr:latest .
fi

docker push 519969025508.dkr.ecr.ap-southeast-2.amazonaws.com/moat_clip_classifier_docker_ecr:latest

aws lambda update-function-code \
  --region ap-southeast-2 \
  --function-name moat_clip_classifier_ecr \
  --image-uri 519969025508.dkr.ecr.ap-southeast-2.amazonaws.com/moat_clip_classifier_docker_ecr:latest \
  --publish