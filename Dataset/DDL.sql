-- Memulai transaksi
BEGIN;

-- Membuat tabel table_m3
CREATE TABLE table_m3 (
    "BPA" VARCHAR(255),
    "Address" VARCHAR(255),
    "Description" TEXT,
    "PTS Existing Units" FLOAT,
    "PTS Proposed Units" FLOAT,
    "Proposed Units" FLOAT,
    "Net Units" FLOAT,
    "Net Units Completed" FLOAT,
    "First Completion Date" VARCHAR(255),
    "Latest Completion Date" VARCHAR(255),
    "Extremely Low Income" INT,
    "Very Low Income" INT,
    "Low Income" INT,
    "Moderate Income" INT,
    "Moderate Income - NDR" INT,
    "Affordable Units" INT,
    "Market Rate" INT,
    "Affordable Units Estimate" VARCHAR(255),
    "Supervisor District" INT,
    "Analysis Neighborhood" VARCHAR(255),
    "Planning Dist." VARCHAR(255),
    "Plan Area" VARCHAR(255),
	"Form No." INT,
    "Permit Type" VARCHAR(255),
    "Issued Date" VARCHAR(255),
    "Authorization Date" VARCHAR(255),
    "Zoning District" VARCHAR(255),
    "Project Affordability Type" VARCHAR(255),
    "BlockLot" VARCHAR(255),
    "PPTS Project ID" VARCHAR(255)
);


-- Menyalin data dari file CSV ke dalam tabel table_m3
COPY table_m3
FROM 'D:\HCK-010\phase_2\Milestone\p2-ftds010-hck-m3-muhamadinsani17\Housing_Production.csv'
DELIMITER ','
CSV HEADER;

-- Menyelesaikan transaksi
COMMIT;

-- Menampilkan semua data dari tabel table_m3
SELECT * FROM table_m3;





