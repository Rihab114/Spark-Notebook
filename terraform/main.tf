provider "google" {
   project     = "golden-system-411709"
   region      = "us-central1"
}

resource "google_compute_network" "vpc_network" {
   name = "spark-vpc"
}
resource "google_compute_subnetwork" "subnetwork" {
   name          = "default-us-east1"
   ip_cidr_range = "10.0.0.0/16"
   network       = google_compute_network.vpc_network.id 
   region        = "us-central1"
}

resource "google_compute_route" "internet_access" {
   name = "default-route"
   dest_range = "0.0.0.0/0"
   network = google_compute_network.vpc_network.id 
   priority = 1000
   next_hop_gateway= "default-internet-gateway"
}

resource "google_compute_firewall" "allow-egress" {
   name = "allow-egress"
   network = google_compute_network.vpc_network.id 
   allow {
       protocol = "tcp"
       ports = ["0-65535"]
   }
   allow {
       protocol = "udp"
       ports = ["0-65535"]
   }
   allow {
       protocol = "icmp"
   }
  direction= "EGRESS"

}

resource "google_compute_firewall" "allow-ingress-ssh" {
   name = "allow-ingress-ssh"
   network = google_compute_network.vpc_network.id 
   allow {
       protocol = "tcp"
       ports = ["22"]
   }
  direction = "INGRESS"
  source_ranges= ["0.0.0.0/0"]
}

resource "google_storage_bucket" "static" {
  name = "air_quality_data_rg_01"
  location = "US"
  storage_class = "STANDARD"

}

resource "google_compute_instance" "dafault" {
   name = "air-quality-compute-engine"
   machine_type = "e2-micro"
   zone = "us-central1-a"
   boot_disk {
    initialize_params {
      image= "debian-cloud/debian-11"
      labels= {
        my_label="value"

      }
    }
   }
  network_interface {
    network= google_compute_network.vpc_network.id 
    subnetwork = google_compute_subnetwork.subnetwork.id
    access_config {
    }
  }
}
