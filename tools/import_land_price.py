import sys
import pandas as pd
import mysql.connector

# Constants
IMPORT_CSV = sys.argv[1]
MYSQL_HOST = sys.argv[2]
MYSQL_USER = sys.argv[3]
MYSQL_PASSWORD = sys.argv[4]
MYSQL_DATABASE = "land_price"
CSV_HEADER = (
    "continuable", "code", "target_year", "standard_land_name", "standard_land_usage_code", "standard_serial_code",
    "city_name", "address", "display_address", "land_price", "previous_year_land_price", "percentage_change_from_previous_year",
    "land_area", "shape", "frontage_ratio", "depth_ratio", "usage_status", "structure", "ground_floor", "under_floor",
    "surrounding_area_status", "front_road_div", "front_road_dir", "front_road_width", "front_road_section_in_front_of_station",
    "side_road_div", "side_road_dir", "gas", "water", "sewer", "nearest_station", "station_proximity_div", "road_distance_to_station",
    "usage_category", "fire_protection_area", "specified_building_coverage", "specified_volume_ratio", "extra_land_area_ratio_div",
    "zone_div", "forest_law", "park_law", "common_spot_div", "previous_code", "previous_time_standard_land_usage_code",
    "previous_time_standard_land_serial_code", "previous_time_land_price", "percentage_change_from_previous_time")

code_df = pd.read_csv(IMPORT_CSV, header=None, names=CSV_HEADER, skiprows=1)

with mysql.connector.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, database=MYSQL_DATABASE) as conn:
    curs = conn.cursor()
    insert_query = """
        INSERT IGNORE INTO land_price (
            continuable,
            code,
            target_year,
            standard_land_name,
            standard_land_usage_code,
            standard_serial_code,
            city_name,
            address,
            display_address,
            land_price,
            previous_year_land_price,
            percentage_change_from_previous_year,
            land_area,
            shape,
            frontage_ratio,
            depth_ratio,
            usage_status,
            structure,
            ground_floor,
            under_floor,
            surrounding_area_status,
            front_road_div,
            front_road_dir,
            front_road_width,
            front_road_section_in_front_of_station,
            side_road_div,
            side_road_dir,
            gas,
            water,
            sewer,
            nearest_station,
            station_proximity_div,
            road_distance_to_station,
            usage_category,
            fire_protection_area,
            specified_building_coverage,
            specified_volume_ratio,
            extra_land_area_ratio_div,
            zone_div,
            forest_law,
            park_law,
            common_spot_div,
            previous_code,
            previous_time_standard_land_usage_code,
            previous_time_standard_land_serial_code,
            previous_time_land_price,
            percentage_change_from_previous_time
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
    """

    for index, row in code_df.iterrows():
        print(index)
        curs.execute(
            insert_query,
            (
                True if row.continuable == "〇" else False,
                row["code"],
                row["target_year"],
                row["standard_land_name"],
                row["standard_land_usage_code"],
                row["standard_serial_code"],
                row["city_name"],
                row["address"],
                row["display_address"] if not pd.isnull(row["display_address"]) else None,
                int(row["land_price"].replace(",", "")) if not pd.isnull(row["land_price"]) else None,
                int(row["previous_year_land_price"].replace(",", "")) if not pd.isnull(row["previous_year_land_price"]) else None,
                row["percentage_change_from_previous_year"] if not pd.isnull(row["percentage_change_from_previous_year"]) else None,
                float(row["land_area"].replace(",", "")) if not pd.isnull(row["land_area"]) else None,
                row["shape"] if not pd.isnull(row["shape"]) else None,
                row["frontage_ratio"],
                row["depth_ratio"],
                row["usage_status"] if not pd.isnull(row["usage_status"]) else None,
                row["structure"] if not pd.isnull(row["structure"]) else None,
                row["ground_floor"] if not pd.isnull(row["ground_floor"]) else None,
                row["under_floor"] if not pd.isnull(row["under_floor"]) else None,
                row["surrounding_area_status"],
                row["front_road_div"] if not pd.isnull(row["front_road_div"]) else None,
                row["front_road_dir"] if not pd.isnull(row["front_road_dir"]) else None,
                row["front_road_width"] if not pd.isnull(row["front_road_width"]) else None,
                row["front_road_section_in_front_of_station"] if not pd.isnull(row["front_road_section_in_front_of_station"]) else None,
                row["side_road_div"] if not pd.isnull(row["side_road_div"]) else None,
                row["side_road_dir"] if not pd.isnull(row["side_road_dir"]) else None,
                True if row["gas"] == "ガス" else False,
                True if row["water"] == "水道" else False,
                True if row["sewer"] == "下水道" else False,
                row["nearest_station"] if not pd.isnull(row["nearest_station"]) else None,
                row["station_proximity_div"] if not pd.isnull(row["station_proximity_div"]) else None,
                int(row["road_distance_to_station"].replace(",", "")) if not pd.isnull(row["road_distance_to_station"]) else None,
                row["usage_category"] if not pd.isnull(row["usage_category"]) else None,
                row["fire_protection_area"] if not pd.isnull(row["fire_protection_area"]) else None,
                row["specified_building_coverage"],
                row["specified_volume_ratio"],
                row["extra_land_area_ratio_div"] if not pd.isnull(row["extra_land_area_ratio_div"]) else None,
                row["zone_div"] if not pd.isnull(row["zone_div"]) else None,
                row["forest_law"] if not pd.isnull(row["forest_law"]) else None,
                row["park_law"] if not pd.isnull(row["park_law"]) else None,
                row["common_spot_div"] if not pd.isnull(row["common_spot_div"]) else None,
                row["previous_code"] if not pd.isnull(row["previous_code"]) else None,
                row["previous_time_standard_land_usage_code"] if not pd.isnull(row["previous_time_standard_land_usage_code"]) else None,
                row["previous_time_standard_land_serial_code"] if not pd.isnull(row["previous_time_standard_land_serial_code"]) else None,
                int(row["previous_time_land_price"].replace(",", "")) if not pd.isnull(row["previous_time_land_price"]) else None,
                row["percentage_change_from_previous_time"] if not pd.isnull(row["percentage_change_from_previous_time"]) else None
            )
        )

    conn.commit()