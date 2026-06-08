# --- Project ---

resource "scaleway_account_project" "project" {
  name = "orchestrator"
}

# --- Security Group ---

resource "scaleway_instance_security_group" "orchestrator" {
  name                    = "orchestrator"
  project_id              = scaleway_account_project.project.id
  inbound_default_policy  = "drop"
  outbound_default_policy = "accept"

  # SSH
  inbound_rule {
    action   = "accept"
    port     = 22
    protocol = "TCP"
  }

  # HTTP (for Let's Encrypt ACME challenge)
  inbound_rule {
    action   = "accept"
    port     = 80
    protocol = "TCP"
  }

  # HTTPS
  inbound_rule {
    action   = "accept"
    port     = 443
    protocol = "TCP"
  }
}

# --- Instance ---

resource "scaleway_instance_ip" "orchestrator" {
  project_id = scaleway_account_project.project.id
}

resource "scaleway_instance_server" "orchestrator" {
  name       = "orchestrator"
  project_id = scaleway_account_project.project.id
  type       = var.instance_type
  image      = var.instance_image

  ip_id             = scaleway_instance_ip.orchestrator.id
  security_group_id = scaleway_instance_security_group.orchestrator.id

  # Minimal cloud-init: install Python for Ansible
  user_data = {
    cloud-init = <<-EOF
      #cloud-config
      package_update: true
      packages:
        - python3
        - python3-apt
    EOF
  }

  tags = ["orchestrator"]
}
