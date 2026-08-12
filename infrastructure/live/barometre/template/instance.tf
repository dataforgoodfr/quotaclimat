# --- GPU VM ---
# Used for local model inference workloads. Regular Scaleway Instances
# auto-inject account-level SSH keys (unlike Elastic Metal — see
# orchestrator/template/instance.tf), so no explicit scaleway_iam_ssh_key
# resource is needed here.

data "scaleway_marketplace_image" "gpu_os" {
  zone          = var.gpu_zone
  label         = var.gpu_image_label
  instance_type = var.gpu_instance_type
  image_type    = "instance_sbs"
}

resource "scaleway_instance_ip" "gpu" {
  zone       = var.gpu_zone
  project_id = scaleway_account_project.project.id
}

resource "scaleway_instance_server" "gpu" {
  name       = "barometre-gpu-${var.environment == "prod" ? "inference" : "training"}-${var.environment}"
  type       = var.gpu_instance_type
  image      = data.scaleway_marketplace_image.gpu_os.id
  zone       = var.gpu_zone
  project_id = scaleway_account_project.project.id
  ip_id      = scaleway_instance_ip.gpu.id

  root_volume {
    volume_type           = "sbs_volume"
    size_in_gb            = var.gpu_root_volume_size_in_gb
    sbs_iops              = 5000
    delete_on_termination = var.environment == "dev" ? true : false
  }

  # Python + uv on every environment; vllm only in prod (dev doesn't need the
  # model-serving stack, just the interpreter/tooling).
  cloud_init = <<-EOF
    #cloud-config
    package_update: true
    packages:
      - python3
      - python3-pip
      - curl
      - ca-certificates
    runcmd:
      - UV_INSTALL_DIR=/usr/local/bin sh -c "curl -LsSf https://astral.sh/uv/install.sh | sh"
      %{if var.environment == "prod"~}
      - /usr/local/bin/uv pip install --system vllm
      %{endif~}
  EOF

  tags = ["barometre", "gpu", var.environment]
}
