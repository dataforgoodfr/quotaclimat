resource "terraform_data" "dgccrf_grants" {
  triggers_replace = [
    scaleway_rdb_user.dgccrf_user.name,
    data.scaleway_rdb_database.barometre.name,
  ]

  provisioner "local-exec" {
    command = <<-EOF
      docker run --rm \
        -e PGPASSWORD="${var.postgres_admin_password}" \
        postgres:15 psql \
          "host=${var.postgres_host} port=${var.postgres_port} dbname=${data.scaleway_rdb_database.barometre.name} user=${var.postgres_admin_user} sslmode=require" \
          -c "GRANT USAGE ON SCHEMA advertising TO \"${scaleway_rdb_user.dgccrf_user.name}\";" \
          -c "GRANT INSERT, UPDATE, DELETE ON TABLE advertising.ad, advertising.ad_occurrence TO \"${scaleway_rdb_user.dgccrf_user.name}\";"
    EOF
  }

  depends_on = [scaleway_rdb_user.dgccrf_user]
}
