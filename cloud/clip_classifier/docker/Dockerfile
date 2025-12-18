# syntax=docker/dockerfile:1

# Base image: AWS Lambda Python 3.12 (x86_64)
FROM public.ecr.aws/lambda/python:3.12

# (Optional) If you know you'll always be in ap-southeast-2, you can set:
ENV AWS_DEFAULT_REGION=ap-southeast-2

# Upgrade pip and install CPU wheels of torch + friends, plus open_clip
# Using the official CPU wheel index to avoid source builds.
RUN python -m pip install --upgrade --no-cache-dir pip && \
    python -m pip install --no-cache-dir \
      --index-url https://download.pytorch.org/whl/cpu \
      torch torchvision torchaudio && \
    python -m pip install --no-cache-dir \
      open_clip_torch Pillow


# Put weights in a stable path
COPY models/ /opt/models/

ENV HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1
# (optional) keep caches pointed somewhere, but we’ll load by path
ENV OPENCLIP_CACHE_DIR=/opt/models


# Pre-download/prime cache into /root/.cache (or another path)
ENV XDG_CACHE_HOME=/root/.cache \
    HF_HOME=/root/.cache/hf \
    HF_HUB_CACHE=/root/.cache/hf/hub \
    TORCH_HOME=/root/.cache/torch \
    HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1 \
    HOME=/root



# Copy your function code
# Ensure your local directory contains lambda_function.py (or adjust CMD below)
COPY . ${LAMBDA_TASK_ROOT}

# Set the handler (module.function)
CMD ["lambda_function.lambda_handler"]