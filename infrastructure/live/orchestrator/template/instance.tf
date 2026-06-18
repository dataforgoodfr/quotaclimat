# --- Project ---

resource "scaleway_account_project" "project" {
  name = "orchestrator"
}

# --- SSH keys ---
# Elastic Metal installs the OS with these keys baked in (no flexible IP / no
# Scaleway auto-injection like instances), so this list is the ONLY way to get
# machine access — keep it in sync with reality or a future apply will reinstall.
# (The ansible `ssh-keys` role still manages root authorized_keys separately and
# needs the same update.)

resource "scaleway_iam_ssh_key" "paul_gabriel" {
  name       = "orchestrator-paul-gabriel"
  public_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIC4YJqfMxnUGCGbzZDZFg3P5B4d2G5DJnaa3WOKXBO99"
}

resource "scaleway_iam_ssh_key" "gmguarino" {
  name       = "orchestrator-gmguarino"
  public_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIIOIZMzVkOuJVUb31YAGMhiKksvLc8r6kqcBMxRPr37l gmguarino1@gmail.com"
}

# --- Bare-metal server (Elastic Metal) ---

data "scaleway_baremetal_offer" "orchestrator" {
  zone = var.zone
  name = var.offer_name
}

data "scaleway_baremetal_os" "orchestrator" {
  zone    = var.zone
  name    = var.os_name
  version = var.os_version
}

resource "scaleway_baremetal_server" "orchestrator" {
  name       = "orchestrator"
  zone       = var.zone
  project_id = scaleway_account_project.project.id

  offer = data.scaleway_baremetal_offer.orchestrator.id
  os    = data.scaleway_baremetal_os.orchestrator.id

  ssh_key_ids = [
    scaleway_iam_ssh_key.paul_gabriel.id,
    scaleway_iam_ssh_key.gmguarino.id,
  ]

  # Elastic Metal has no instance security group — UFW (ansible common role) is
  # the firewall. Storage is the local NVMe (no attachable block volume).
  # Minimal cloud-init so Ansible has Python available.
  cloud_init = <<-EOF
    #cloud-config
    package_update: true
    packages:
      - python3
      - python3-apt
  EOF

  tags = ["orchestrator"]
}
