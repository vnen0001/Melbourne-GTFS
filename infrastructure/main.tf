# Creating a Compute Enginer Instance

resource "google_compute_instance" "melb-gtfs-vm" {
  name         = "melb-gtfs-kafka"
  machine_type = var.machine_type
  tags         = ["kafka-instance"]


  zone = var.zone

  boot_disk {
    initialize_params {
      image = var.image_vm
      size  = var.disk_size
      type  = "pd-standard"
    }
  }
  network_interface {
    network = "default"
    access_config {
      network_tier = "STANDARD"
    }
    queue_count = 0 # 0 menans GCP will decide the number of network transmit and recieve queues 
    stack_type  = "IPV4_ONLY"
    subnetwork  = "projects/${var.project_id}/regions/${join("-", slice(split("-", var.zone), 0, 2))}/subnetworks/default" # this would dynamically detemrine the subnetwork bases on Project ID and region 

  }
  # This will define how my system would behave on startup, maintenance, disruption 
  scheduling {
    automatic_restart   = true       # This would start the VM automatically
    on_host_maintenance = "MIGRATE"  # This would migrate the instance during maintenance
    preemptible         = false      # This means GCP cannot stop my instance at any time
    provisioning_model  = "STANDARD" # Use standard for produciton and spot for cost savings
  }
  shielded_instance_config {
    enable_secure_boot = false # True only for highly secure environment
  }
  service_account {
    email  = var.gcp_service_account
    scopes = ["cloud-platform"]
  }

  # Adding public key 
  metadata = {
    ssh-keys = "${var.ssh_user}:${var.ssh_key}"
  }

}

# Firewall setup 

resource "google_compute_firewall" "default" {
  name    = "fireall-for-kafka"
  network = "default"
  allow {
    protocol = "icmp"
  }
  allow {
    protocol = "tcp"
    ports    = ["9092", "2181", "9093", "2888", "3888"] # kafka broker, zookeeper, additional, zookekeper quorum
  }
  allow {
    protocol = "tcp"
    ports    = ["443"]
  }

  source_ranges = ["0.0.0.0/0"]
}

# Generating a .ini  file for ansible 
resource "local_file" "ansible-inventory" {
  content  = <<-EOF
[kafka]
${google_compute_instance.melb-gtfs-vm.network_interface[0].access_config[0].nat_ip} ansible_ssh_user=${var.ssh_user} ansible_ssh_private_key_file=${var.ssh_key}
EOF
  filename = "${path.module}/ansible_automation_kafka/inventory.ini"
}
