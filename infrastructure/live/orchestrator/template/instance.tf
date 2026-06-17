# --- Project ---

resource "scaleway_account_project" "project" {
  name = "orchestrator"
}

# --- SSH keys ---
# Elastic Metal installs the OS with these keys baked in (no flexible IP / no
# Scaleway auto-injection like instances). Public keys mirror the deploy keys the
# ansible `ssh-keys` role manages on the host.

resource "scaleway_iam_ssh_key" "paul" {
  name       = "orchestrator-paul"
  public_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBXjyIJ/Kp3iSV10p+zZcPSXVqk/1CMbyY9z89XdijhM paul@dataforgood.fr"
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
    scaleway_iam_ssh_key.paul.id,
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
