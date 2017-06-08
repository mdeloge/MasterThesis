CREATE TABLE `thesis`.`template` (
  `Datetime` DATETIME NOT NULL,
  `Consumption` FLOAT NOT NULL,
  `MeterId` VARCHAR(45) NOT NULL,
  `MeterName` VARCHAR(45) NOT NULL,
  `Type` VARCHAR(45) NOT NULL,
  `Unit` VARCHAR(45) NOT NULL,
  `RecordNumber` VARCHAR(45) NOT NULL,
  `RecordName` VARCHAR(45) NOT NULL,
  `DwellingType` VARCHAR(45) NOT NULL,
  `ConstructionYear` VARCHAR(45) NOT NULL,
  `RenovationYear` VARCHAR(45) NOT NULL,
  `Country` VARCHAR(45) NOT NULL,
  `PostalCode` INT NOT NULL,
  `FloorSurface` INT NOT NULL,
  `HouseholdSize` INT NOT NULL,
  `HeatingOn` VARCHAR(45) NOT NULL,
  `CookingOn` VARCHAR(45) NOT NULL,
  `HotWaterOn` VARCHAR(45) NOT NULL,
  `EnergyPerformance` INT NOT NULL,
  `EnergyRating` INT NOT NULL,
  `Category` VARCHAR(45) NOT NULL,
  `EnergyEfficiency` VARCHAR(45) NOT NULL,
  `AuxiliaryHeatingOn` VARCHAR(45) NOT NULL,
  `Installations` VARCHAR(45) NOT NULL,
  `StreetAddress` VARCHAR(45) NOT NULL,
  `Email` VARCHAR(45) NOT NULL,
  `BusinessName` VARCHAR(45) NOT NULL,
  `FullName` VARCHAR(45) NOT NULL,
  `Multiplier` INT NOT NULL,
  `ReadingType` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`Datetime`, `MeterId`));