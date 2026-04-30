variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "datasets" {
  type = list(string)
  default = [
    "coffee_raw",
    "coffee_dev",
    "coffee_prod"
  ]
}