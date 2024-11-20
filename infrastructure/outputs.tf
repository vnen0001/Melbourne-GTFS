output "public_ip" {
    description = "Displays the public IP of the VM created"
    value = google_compute_instance.melb-gtfs-vm.network_interface[0].access_config[0].nat_ip
}

output "ssh_user" {
    value = var.ssh_user
}

output "private_key_path" {
    value = var.ssh_key
}


output "database-ip" {
    value =  google_sql_database_instance.postgres.public_ip_address
}

output "database-name" {
    value = google_sql_database.post-db.name
}