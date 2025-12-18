from huggingface_hub import hf_hub_download
p = hf_hub_download(
    repo_id="laion/CLIP-ViT-B-32-laion2B-s34B-b79K",
    filename="open_clip_pytorch_model.bin",
    local_dir="models/CLIP-ViT-B-32-laion2b_s34b_b79k",
    local_dir_use_symlinks=False,
)
print("Saved to:", p)