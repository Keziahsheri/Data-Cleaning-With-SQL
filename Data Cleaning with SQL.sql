DROP TABLE IF EXISTS project.Housing_data;
CREATE TABLE project.Housing_data(
UniqueID INT,
ParcelID TEXT,
Landuse TEXT(100),
PropertyAddress TEXT(100),
SaleDate TEXT,
SalePrice TEXT(100),
LegalReference INT,
SoldasVacant  TEXT(100),
OwnerName TEXT(100),
OwnerAddress TEXT(100),
Acreage TEXT(100),
TaxDistrict TEXT(100),
LandValue TEXT(100),
BuildingValue TEXT(100),
TotalValUe TEXT(100),
YearBuilt TEXT,
Bedrooms TEXT(100),
FullBath INT,
HalfBath INT
);

LOAD DATA LOCAL INFILE "C:/Program Files/MySQL/MySQL Server 8.0/Uploads/Nashville Housing Data for Data Cleaning (reuploaded).csv" INTO TABLE project.Housing_data
FIELDS TERMINATED BY ','
ENCLOSED by '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(UniqueID,ParcelID,Landuse,PropertyAddress,SaleDate,SalePrice,LegalReference,SoldasVacant,OwnerName,OwnerAddress,Acreage,TaxDistrict,LandValue,BuildingValue,TotalValUe,YearBuilt,Bedrooms,FullBath,HalfBath);

-- Preview the housing data
SELECT *
FROM project.housing_data;

-- Standardize and update the Date format
SELECT DATE_FORMAT(STR_TO_DATE(SaleDate, '%M %e, %Y'), '%Y-%m-%d') AS SaleDate
FROM project.housing_data;


SET SQL_SAFE_UPDATES = 0;
UPDATE  project.housing_data
SET  SaleDate = DATE_FORMAT(STR_TO_DATE(SaleDate, '%M %e, %Y'), '%Y-%m-%d');
SET SQL_SAFE_UPDATES = 1;

-- Populate Property Address data
SELECT *
FROM project.housing_data
WHERE PropertyAddress = '';

-- Replace the empty spaces with NULL

SET SQL_SAFE_UPDATES = 0;
UPDATE project.housing_data
SET propertyaddress= NULL
WHERE Propertyaddress = '';
SET SQL_SAFE_UPDATES = 1;



SELECT *
FROM project.housing_data
WHERE PropertyAddress is NULL;

SELECT a.parcelID,a.propertyaddress,b.parcelID, b.PropertyAddress
FROM Project.housing_data AS a
JOIN Project.housing_data AS b
	ON a.ParcelID = b.ParcelID
	AND a.UniqueID <> b.UniqueID
WHERE a.PropertyAddress IS NULL;

-- To populate;
SELECT a.parcelID,a.propertyaddress,b.parcelID, b.PropertyAddress, IFNULL(a.propertyaddress,b.propertyaddress)
FROM Project.housing_data AS a
JOIN Project.housing_data AS b
	ON a.ParcelID = b.ParcelID
	AND a.UniqueID <> b.UniqueID
WHERE a.PropertyAddress IS NULL;


SET SQL_SAFE_UPDATES = 0;
UPDATE project.housing_data a
JOIN project.housing_data b
ON a.ParcelID = b.ParcelID
AND a.UniqueID <> b.UniqueID
SET a.PropertyAddress = IFNULL(a.PropertyAddress, b.PropertyAddress)
WHERE a.PropertyAddress IS NULL;
SET SQL_SAFE_UPDATES = 1;

-- Split the address into individual columns i.e Address, city, state
-- USE SUBSTRING_INDEX(string, delimiter, count)


Select Propertyaddress,
SUBSTRING_INDEX(propertyaddress,',',1) as Propertysplitaddress,
SUBSTRING_INDEX(SUBSTRING_INDEX(propertyaddress,',',-1), ',',1) as propertysplitcity
From Project.housing_data;


Select owneraddress,
SUBSTRING_INDEX(owneraddress,',',1) as ownersplitaddress,
SUBSTRING_INDEX(SUBSTRING_INDEX(owneraddress,',',-2), ',',1) as ownersplitcity,
SUBSTRING_INDEX(owneraddress, ', ', -1) AS ownersplitstate
From Project.housing_data;


ALTER TABLE project.housing_data
ADD propertysplitaddress VARCHAR(255);

SET SQL_SAFE_UPDATES = 0;
UPDATE project.housing_data
SET propertysplitaddress= SUBSTRING_INDEX(propertyaddress,',',1);
SET SQL_SAFE_UPDATES = 1;


ALTER TABLE project.housing_data
ADD propertysplitcity VARCHAR(255);

SET SQL_SAFE_UPDATES = 0;
UPDATE project.housing_data
SET propertysplitcity= SUBSTRING_INDEX(SUBSTRING_INDEX(propertyaddress,',',-1), ',',1);
SET SQL_SAFE_UPDATES = 1;

ALTER TABLE project.housing_data
ADD ownersplitaddress VARCHAR(255);

SET SQL_SAFE_UPDATES = 0;
UPDATE project.housing_data
SET ownersplitaddress= SUBSTRING_INDEX(owneraddress,',',1);
SET SQL_SAFE_UPDATES = 1;


ALTER TABLE project.housing_data
ADD ownersplitcity VARCHAR(255);

SET SQL_SAFE_UPDATES = 0;
UPDATE project.housing_data
SET ownersplitcity= SUBSTRING_INDEX(SUBSTRING_INDEX(owneraddress,',',-2), ',',1);
SET SQL_SAFE_UPDATES = 1;


ALTER TABLE project.housing_data
ADD ownersplitstate VARCHAR(255);

SET SQL_SAFE_UPDATES = 0;
UPDATE project.housing_data
SET ownersplitstate= SUBSTRING_INDEX(owneraddress, ', ', -1);
SET SQL_SAFE_UPDATES = 1;

-- Change N and Y in the soldasvacant field to 'NO' and 'YES'

SELECT DISTINCT(Soldasvacant), COUNT(Soldasvacant)
From Project.housing_data
GROUP BY Soldasvacant
ORDER BY 2;

SELECT Soldasvacant,
CASE WHEN Soldasvacant = 'Y' THEN 'Yes'
WHEN Soldasvacant = 'N' THEN 'No'
ELSE SoldasVacant
END 
FROM Project.housing_data;


SET SQL_SAFE_UPDATES = 0;

UPDATE Project.housing_data
SET Soldasvacant = CASE WHEN Soldasvacant = 'Y' THEN 'Yes'
WHEN Soldasvacant = 'N' THEN 'No'
ELSE SoldasVacant
END
;
SET SQL_SAFE_UPDATES = 1;


-- Look out for Duplicates
-- Make use of Row_Number

SELECT *,
	ROW_NUMBER() OVER 
    (PARTITION BY 	parcelID,
					propertyaddress,
                    Saleprice,
                    Saledate,
                    LegalReference
                    ORDER BY UniqueID) AS row_num
FROM Project.housing_data
ORDER BY ParcelID;

-- Duplicates using CTE

WITH Rownum_CTE AS(
SELECT *,
	ROW_NUMBER() OVER 
    (PARTITION BY 	parcelID,
					propertyaddress,
                    Saleprice,
                    Saledate,
                    LegalReference
                    ORDER BY UniqueID) AS row_num
FROM Project.housing_data)
SELECT *
FROM Rownum_CTE
WHERE row_num>1
ORDER BY Propertyaddress;


-- To delete the duplicates


SET SQL_SAFE_UPDATES = 0;
DELETE FROM Project.housing_data
WHERE UniqueID NOT IN (
    SELECT * FROM (
        SELECT MIN(UniqueID)
        FROM Project.housing_data
        GROUP BY parcelID, propertyaddress, Saleprice, Saledate, LegalReference
    ) AS temp
);
SET SQL_SAFE_UPDATES = 1;


-- DELETE UNUSED COLUMNS

ALTER TABLE Project.housing_data
DROP COLUMN Propertyaddress,
DROP COLUMN Owneraddress,
DROP COLUMN taxdistrict;

SELECT * 
FROM Project.housing_data;