variable "project_id" {
    description = "The selected project id for GCP"
    type =  string
}

variable "region" {
    description = "The region of GCP"
    type = string
    default = "us-central1"
}
variable "zone" {
    description = "The zone in which we want resources"
    type = string

}
variable "machine_type" {
    description = "The type of VM needed"
    type = string
}
variable "image_vm" {
    description = "The OS image for our VM"
    type = string
    default = "ubuntu-os-cloud/ubuntu-2204-lts"
}

variable "disk_size" {
    description = "The size of disk"  
    type = number
}
variable "gcp_service_account" {
    type = string
    description = "The service account to use"
}

variable "ssh_key" {
    description = "The public key for VM"
    type = string
}
variable "ssh_user" {
    type = string  
}

variable "my-local-ip" {
    type = string
  
}
variable "database-user" {
    type = string
  
}
variable "database-password" {
  type = string
}