CREATE DATABASE IF NOT EXISTS wells_db CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE wells_db;

CREATE TABLE IF NOT EXISTS wells (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,

  operator_company VARCHAR(255),
  well_name_number VARCHAR(255),
  api_number VARCHAR(32),
  job_type VARCHAR(128),
  address TEXT,
  longitude DECIMAL(10,6),
  latitude DECIMAL(9,6),
  date_stimulated VARCHAR(32),
  stimulated_formation VARCHAR(128),
  top_ft DECIMAL(10,2),
  bottom_ft DECIMAL(10,2),
  stimulation_stages INT,
  volume_value DECIMAL(12,2),
  volume_units VARCHAR(16),
  treatment_type VARCHAR(128),
  acid_percent DECIMAL(5,2),
  lbs_proppant DECIMAL(12,2),
  max_treatment_pressure_psi DECIMAL(12,2),
  max_treatment_rate_bbls_per_min DECIMAL(12,2),
  details TEXT

  -- Columns needed after web scraping
  status VARCHAR(50) DEFAULT NULL,
  type VARCHAR(50) DEFAULT NULL,
  city VARCHAR(100) DEFAULT NULL,
  lat DECIMAL(10,6) DEFAULT NULL,
  lon DECIMAL(10,6) DEFAULT NULL;

  oil_bbl INT DEFAULT NULL,
  oil_desc VARCHAR(255) DEFAULT NULL,
  gas_bbl INT DEFAULT NULL,
  gas_desc VARCHAR(255) DEFAULT NULL
);