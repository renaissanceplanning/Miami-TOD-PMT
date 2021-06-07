"""
Configuration for downloading census geographic data from the Census.gov TIGER data FTP site
"""
FTP_HOME = r"ftp://ftp2.census.gov/geo/tiger/TIGER2019/"

GEO_TYPES_DICT = {
    'cd': 'Congressional Districts',
    'county': 'Counties',
    'tabblock': 'Census Blocks',
    'bg': 'Block Groups',
    'elsd': 'Elementary School Districts',
    'place': 'Places',
    'scsd': 'Secondary School Districts',
    'sldl': 'State Legislative Districts Lower',
    'sldu': 'State Legislative Districts Upper',
    'state': 'States',
    'unsd': 'Unified School Districts',
    'zcta5': '5-Digit Zip Code Tabulation Area',
}

GEO_TYPES_LIST = sorted([
    key for key, value in GEO_TYPES_DICT.items()
])

# The zcta5 file is 500 Mb. DISABLE_AUTO_DOWNLOADS prevents it from being
# fetched automatically if someone runs `fetch_shapefiles.py` with no args.
# If you do want the Zip Code Tabulation Area shapefile, target specifically:
# >> python fetch_shapefiles.py -g zcta5
DISABLE_AUTO_DOWNLOADS = ['zcta5', ]

STATE_FIPS_DICT = {
    '01': {
        'abbreviation': 'AL', 'name': 'Alabama',
        'region': 3, 'region_name': 'South',
        'division': 6, 'division_name': 'East South Central',
    },
    '02': {
        'abbreviation': 'AK', 'name': 'Alaska',
        'region': 4, 'region_name': 'West',
        'division': 9, 'division_name': 'Pacific',
    },
    '04': {
        'abbreviation': 'AZ', 'name': 'Arizona',
        'region': 4, 'region_name': 'West',
        'division': 8, 'division_name': 'Mountain',
    },
    '05': {
        'abbreviation': 'AR', 'name': 'Arkansas',
        'region': 3, 'region_name': 'South',
        'division': 7, 'division_name': 'West South Central',
    },
    '06': {
        'abbreviation': 'CA', 'name': 'California',
        'region': 4, 'region_name': 'West',
        'division': 9, 'division_name': 'Pacific',
    },
    '08': {
        'abbreviation': 'CO', 'name': 'Colorado',
        'region': 4, 'region_name': 'West',
        'division': 8, 'division_name': 'Mountain',
    },
    '09': {
        'abbreviation': 'CT', 'name': 'Connecticut',
        'region': 1, 'region_name': 'Northeast',
        'division': 1, 'division_name': 'New England',
    },
    '10': {
        'abbreviation': 'DE', 'name': 'Delaware',
        'region': 3, 'region_name': 'South',
        'division': 5, 'division_name': 'South Atlantic',
    },
    '11': {
        'abbreviation': 'DC', 'name': 'District of Columbia',
        'region': 3, 'region_name': 'South',
        'division': 5, 'division_name': 'South Atlantic',
    },
    '12': {
        'abbreviation': 'FL', 'name': 'Florida',
        'region': 3, 'region_name': 'South',
        'division': 5, 'division_name': 'South Atlantic',
    },
    '13': {
        'abbreviation': 'GA', 'name': 'Georgia',
        'region': 3, 'region_name': 'South',
        'division': 5, 'division_name': 'South Atlantic',
    },
    '15': {
        'abbreviation': 'HI', 'name': 'Hawaii',
        'region': 4, 'region_name': 'West',
        'division': 9, 'division_name': 'Pacific',
    },
    '16': {
        'abbreviation': 'ID', 'name': 'Idaho',
        'region': 4, 'region_name': 'West',
        'division': 8, 'division_name': 'Mountain',
    },
    '17': {
        'abbreviation': 'IL', 'name': 'Illinois',
        'region': 2, 'region_name': 'Midwest',
        'division': 3, 'division_name': 'East North Central',
    },
    '18': {
        'abbreviation': 'IN', 'name': 'Indiana',
        'region': 2, 'region_name': 'Midwest',
        'division': 3, 'division_name': 'East North Central',
    },
    '19': {
        'abbreviation': 'IA', 'name': 'Iowa',
        'region': 2, 'region_name': 'Midwest',
        'division': 4, 'division_name': 'West North Central',
    },
    '20': {
        'abbreviation': 'KS', 'name': 'Kansas',
        'region': 2, 'region_name': 'Midwest',
        'division': 4, 'division_name': 'West North Central',
    },
    '21': {
        'abbreviation': 'KY', 'name': 'Kentucky',
        'region': 3, 'region_name': 'South',
        'division': 6, 'division_name': 'East South Central',
    },
    '22': {
        'abbreviation': 'LA', 'name': 'Louisiana',
        'region': 3, 'region_name': 'South',
        'division': 7, 'division_name': 'West South Central',
    },
    '23': {
        'abbreviation': 'ME', 'name': 'Maine',
        'region': 1, 'region_name': 'Northeast',
        'division': 1, 'division_name': 'New England',
    },
    '24': {
        'abbreviation': 'MD', 'name': 'Maryland',
        'region': 3, 'region_name': 'South',
        'division': 5, 'division_name': 'South Atlantic',
    },
    '25': {
        'abbreviation': 'MA', 'name': 'Massachusetts',
        'region': 1, 'region_name': 'Northeast',
        'division': 1, 'division_name': 'New England',
    },
    '26': {
        'abbreviation': 'MI', 'name': 'Michigan',
        'region': 2, 'region_name': 'Midwest',
        'division': 3, 'division_name': 'East North Central',
    },
    '27': {
        'abbreviation': 'MN', 'name': 'Minnesota',
        'region': 2, 'region_name': 'Midwest',
        'division': 4, 'division_name': 'West North Central',
    },
    '28': {
        'abbreviation': 'MS', 'name': 'Mississippi',
        'region': 3, 'region_name': 'South',
        'division': 6, 'division_name': 'East South Central',
    },
    '29': {
        'abbreviation': 'MO', 'name': 'Missouri',
        'region': 2, 'region_name': 'Midwest',
        'division': 4, 'division_name': 'West North Central',
    },
    '30': {
        'abbreviation': 'MT', 'name': 'Montana',
        'region': 4, 'region_name': 'West',
        'division': 8, 'division_name': 'Mountain',
    },
    '31': {
        'abbreviation': 'NE', 'name': 'Nebraska',
        'region': 2, 'region_name': 'Midwest',
        'division': 4, 'division_name': 'West North Central',
    },
    '32': {
        'abbreviation': 'NV', 'name': 'Nevada',
        'region': 4, 'region_name': 'West',
        'division': 8, 'division_name': 'Mountain',
    },
    '33': {
        'abbreviation': 'NH', 'name': 'New Hampshire',
        'region': 1, 'region_name': 'Northeast',
        'division': 1, 'division_name': 'New England',
    },
    '34': {
        'abbreviation': 'NJ', 'name': 'New Jersey',
        'region': 1, 'region_name': 'Northeast',
        'division': 2, 'division_name': 'Middle Atlantic',
    },
    '35': {
        'abbreviation': 'NM', 'name': 'New Mexico',
        'region': 4, 'region_name': 'West',
        'division': 8, 'division_name': 'Mountain',
    },
    '36': {
        'abbreviation': 'NY', 'name': 'New York',
        'region': 1, 'region_name': 'Northeast',
        'division': 2, 'division_name': 'Middle Atlantic',
    },
    '37': {
        'abbreviation': 'NC', 'name': 'North Carolina',
        'region': 3, 'region_name': 'South',
        'division': 5, 'division_name': 'South Atlantic',
    },
    '38': {
        'abbreviation': 'ND', 'name': 'North Dakota',
        'region': 2, 'region_name': 'Midwest',
        'division': 4, 'division_name': 'West North Central',
    },
    '39': {
        'abbreviation': 'OH', 'name': 'Ohio',
        'region': 2, 'region_name': 'Midwest',
        'division': 3, 'division_name': 'East North Central',
    },
    '40': {
        'abbreviation': 'OK', 'name': 'Oklahoma',
        'region': 3, 'region_name': 'South',
        'division': 7, 'division_name': 'West South Central',
    },
    '41': {
        'abbreviation': 'OR', 'name': 'Oregon',
        'region': 4, 'region_name': 'West',
        'division': 9, 'division_name': 'Pacific',
    },
    '42': {
        'abbreviation': 'PA', 'name': 'Pennsylvania',
        'region': 1, 'region_name': 'Northeast',
        'division': 2, 'division_name': 'Middle Atlantic',
    },
    '44': {
        'abbreviation': 'RI', 'name': 'Rhode Island',
        'region': 1, 'region_name': 'Northeast',
        'division': 1, 'division_name': 'New England',
    },
    '45': {
        'abbreviation': 'SC', 'name': 'South Carolina',
        'region': 3, 'region_name': 'South',
        'division': 5, 'division_name': 'South Atlantic',
    },
    '46': {
        'abbreviation': 'SD', 'name': 'South Dakota',
        'region': 2, 'region_name': 'Midwest',
        'division': 4, 'division_name': 'West North Central',
    },
    '47': {
        'abbreviation': 'TN', 'name': 'Tennessee',
        'region': 3, 'region_name': 'South',
        'division': 6, 'division_name': 'East South Central',
    },
    '48': {
        'abbreviation': 'TX', 'name': 'Texas',
        'region': 3, 'region_name': 'South',
        'division': 7, 'division_name': 'West South Central',
    },
    '49': {
        'abbreviation': 'UT', 'name': 'Utah',
        'region': 4, 'region_name': 'West',
        'division': 8, 'division_name': 'Mountain',
    },
    '50': {
        'abbreviation': 'VT', 'name': 'Vermont',
        'region': 1, 'region_name': 'Northeast',
        'division': 1, 'division_name': 'New England',
    },
    '51': {
        'abbreviation': 'VA', 'name': 'Virginia',
        'region': 3, 'region_name': 'South',
        'division': 5, 'division_name': 'South Atlantic',
    },
    '53': {
        'abbreviation': 'WA', 'name': 'Washington',
        'region': 4, 'region_name': 'West',
        'division': 9, 'division_name': 'Pacific',
    },
    '54': {
        'abbreviation': 'WV', 'name': 'West Virginia',
        'region': 3, 'region_name': 'South',
        'division': 5, 'division_name': 'South Atlantic',
    },
    '55': {
        'abbreviation': 'WI', 'name': 'Wisconsin',
        'region': 2, 'region_name': 'Midwest',
        'division': 3, 'division_name': 'East North Central',
    },
    '56': {
        'abbreviation': 'WY', 'name': 'Wyoming',
        'region': 4, 'region_name': 'West',
        'division': 8, 'division_name': 'Mountain',
    },
    '60': {
        'abbreviation': 'AS', 'name': 'America Samoa',
        'region': None, 'region_name': '',
        'division': None, 'division_name': '',
    },
    '66': {
        'abbreviation': 'GU', 'name': 'Guam',
        'region': None, 'region_name': '',
        'division': None, 'division_name': '',
    },
    '69': {
        'abbreviation': 'MP', 'name': 'Commonwealth of the Northern Mariana Islands',
        'region': None, 'region_name': '',
        'division': None, 'division_name': '',
    },
    '72': {
        'abbreviation': 'PR', 'name': 'Puerto Rico',
        'region': None, 'region_name': '',
        'division': None, 'division_name': '',
    },
    '78': {
        'abbreviation': 'VI', 'name': 'United States Virgin Islands',
        'region': None, 'region_name': '',
        'division': None, 'division_name': '',
    },
}


def get_fips_code_for_state(state):
    for key, state_dict in STATE_FIPS_DICT.items():
        if state_dict['abbreviation'] == state.upper() \
                or state_dict['name'].upper() == state.upper():
            return key


STATE_ABBREV_LIST = sorted([
    state['abbreviation'] for fips, state in STATE_FIPS_DICT.items()
])

# Sources:
# http://mcdc2.missouri.edu/pub/data/sf32000/Techdoc/ch4_summary_level_seq_chart.pdf
# http://www2.census.gov/acs2011_1yr/summaryfile/ACS_2011_SF_Tech_Doc.pdf
SUMMARY_LEVEL_DICT = {
    "010", "United States",
    "020", "Region",
    "030", "Division",
    "040", "State",
    "050", "State-County",
    "060", "State-County-County Subdivision",
    "061", "Minor Civil Division (MCD)/Census County Division (CCD) (10,000+)",
    "062", "Minor Civil Division (MCD)/Census County Division (CCD) (<10,000)",
    "063", "Minor Civil Division (MCD)/Census County Division (CCD) (2500+)",
    "064", "Minor Civil Division (MCD)/Census County Division (CCD) (< 2500 in Metro Area)",
    "067", "State (Puerto Rico Only)-County-County Subdivision-Subbarrio",
    "070", "State-County-County Subdivision-Place/Remainder",
    "071", "County Subdivision-Place (10,000+)/Remainder",
    "072", "County Subdivision-Place (2500+)/Remainder",
    "080", "State-County-County Subdivision-Place/Remainder-Census Tract",
    "082", "County Subdivision-Place(2500+)/Remainder-Census Tract",
    "085", "State-County-County Subdivision-Place/Remainder-Census Tract-Urban/Rural",
    "090", "State-County-County Subdivision-Place/Remainder-Census Tract-Urban/Rural-Block Group",
    "091", "County Subdivision-Place/Remainder-Census Tract-Block Group",
    "101", "State-County-Census Tract-Block",
    "140", "State-County-Census Tract",
    "144", "State-County-Census Tract-American Indian Area/Alaska Native Area/Hawaiian Home Land",
    "150", "State-County-Census Tract-Block Group",
    "154", "State-County-Census Tract-Block Group-American Indian Area/Alaska Native Area/Hawaiian Home Land",
    "155", "State-Place-County",
    "157", "State-Place (no CDPs)-County",
    "158", "State-Place-County-Census Tract",
    "160", "State-Place",
    "161", "State-Place (10,000+)",
    "162", "State-Place (no CDPs)",
    "170", "State-Consolidated City",
    "172", "State-Consolidated City-Place Within Consolidated City",
    "200", "American Indian Reservation with Trust Lands",
    "201", "American Indian Reservation with Trust Lands: Reservation Only",
    "202", "American Indian Reservations with Trust Lands: Trust Lands Only",
    "203", "American Indian Reservation No Trust Lands/Tribal Jurisdiction Sa/Etc",
    "204", "American Indian Trust Lands (With No Reservation)",
    "205", "American Indian Reservation with Trust Lands: Reservation Only-State",
    "206", "American Indian Reservation with Trust Lands: Trust Lands Only-State",
    "207", "American Indian Reservation No Trust Lands/Tribal Jurisdiction Sa/Etc-State",
    "208", "American Indian Trust Lands (With No Reservation)-State",
    "210", "State-American Indian Reservation",
    "211", "State-American Indian Reservation Only",
    "212", "State-American Indian Reservation Trust Land Only",
    "215", "State-American Indian Reservation Jurisdiction",
    "216", "State-American Indian Trust Lands",
    "220", "American Indian Reservation Jurisdiction-Co",
    "221", "American Indian Trust Lands Only-Co",
    "230", "State-Alaska Native Regional Corporation",
    "250", "American Indian Area/Alaska Native Area/Hawaiian Home Land",
    "252", "American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)",
    "251", "American Indian Area/Alaska Native Area/Hawaiian Home Land-Tribal Subdivision/Remainder",
    "253",
    "American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)-Tribal Subdivision/Remainder",
    "254", "American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land",
    "255", "American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land-Tribal Subdivision/Remainder",
    "256", "Specified American Indian Area-Tribal Census Tract",
    "257", "Specified American Indian Area-Tribal Subdivision/Remainder-Tribal Census Tract",
    "259", "Specified American Indian Area-Tribal Subdivision/Remainder-Tribal Census Tract-Tribal Block Group",
    "258", "Specified American Indian Area-Tribal Census Tract-Tribal Block Group",
    "259", "Specified American Indian Area-Tribal Subdivision/Remainder-Tribal Census Tract-Tribal Block Group",
    "260", "American Indian Area/Alaska Native Area/Hawaiian Home Land-State",
    "261", "State-American Indian Area/Alaska Native Area/Hawaiian Home Land-County-County Subdivision",
    "262", "American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)-State",
    "263", "State-American Indian Area/Alaska Native Area/Hawaiian Home Land-County-County Subdivision-Place/Remainder",
    "264", "American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land-State",
    "265",
    "State-American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)-County-County Subdivision",
    "266",
    "State-American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)-County-County Subdivision-Place/Remainder",
    "267", "State-American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land-County-County Subdivision",
    "268",
    "State-American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land-County-County Subdivision-Place/Remainder",
    "269", "American Indian Area/Alaska Native Area/Hawaiian Home Land-Place-Remainder",
    "270", "American Indian Area/Alaska Native Area/Hawaiian Home Land-State-County",
    "271", "American Indian Area/Alaska Native Area/Hawaiian Home Land-State-County-County Subdivision ",
    "272", "American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)-State-County",
    "273",
    "American Indian Area/Alaska Native Area/Hawaiian Home Land-State-County-County Subdivision-Place/Remainder ",
    "274", "American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land-State-County",
    "275",
    "American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)-State-County-County Subdivision",
    "276",
    "American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)-State-County-County Subdivision-Place/Remainder",
    "277", "American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land-State-County-County Subdivision ",
    "278",
    "American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land-State-County-County Subdivision-Place/Remainder",
    "280", "State-American Indian Area/Alaska Native Area/Hawaiian Home Land",
    "281", "State-AmericanIndianArea/AlaskaNativeArea/Hawaiian Home Land-Tribal Subdivision/Remainder",
    "282", "State-American Indian Area/Alaska Native Area/Hawaiian Home Land-County",
    "283", "State-American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)",
    "284",
    "State-American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)-Tribal Subdivision/Remainder",
    "285", "State-American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)-County",
    "286", "State-American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land",
    "287",
    "State-American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land-Tribal Subdivision/Remainder",
    "288", "State-American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land-County",
    "290", "American Indian Area/Alaska Native Area/Hawaiian Home Land-Tribal Subdivision/Remainder-State",
    "291", "Specified American Indian Area (Reservation Only)-Tribal Census Tract",
    "292", "Specified American Indian Area (Off-Reservation Trust Land Only)-Tribal Census Tract",
    "293", "Specified American Indian Area (Reservation Only)-Tribal Census Tract-Tribal Block Group",
    "294", "Specified American Indian Area (Off-Reservation Trust Land Only)-Tribal Census Tract-Tribal Block Group",
    "300", "Metropolitan Statistical Area (MSA)/Consolidated Metropolitan Statistical Area (CMSA)",
    "301", "Primary Metropolitan Statistical Area",
    "310", "Core Based Statistical Area (CBSA)",
    "311", "Core Based Statistical Area (CBSA)-State",
    "312", "Core Based Statistical Area (CBSA)-State-Principal City",
    "313", "Core Based Statistical Area (CBSA)-State-County",
    "314", "Metropolitan Statistical Area (MSA)/Metropolitan Division",
    "315", "Metropolitan Statistical Area (MSA)/Metropolitan Division-State",
    "316", "Metropolitan Statistical Area (MSA)/Metropolitan Division-State-County",
    "319", "State-Metropolitan Statistical Area (MSA)/Consolidated Metropolitan Statistical Area (CMSA)",
    "320", "State-Core Based Statistical Area (CBSA)",
    "321", "State-Core Based Statistical Area (CBSA)-Principal City",
    "322", "State-Core Based Statistical Area (CBSA)-County",
    "323", "State-Metropolitan Statistical Area (MSA)/Metropolitan Division",
    "324", "State-Metropolitan Statistical Area (MSA)/Metropolitan Division-County",
    "329", "Metropolitan Statistical Area (MSA) (no CMSAs)-State-County",
    "330", "Combined Statistical Area (CSA)",
    "331", "Combined Statistical Area (CSA)-State",
    "332", "Combined Statistical Area (CSA)-Core Based Statistical Area (CBSA)",
    "333", "Combined Statistical Area (CSA)-Core Based Statistical Area (CBSA)-State",
    "335", "Combined New England City and Town Area",
    "336", "Combined New England City and Town Area-State",
    "337", "Combined New England City and Town Area-New England City and Town Area (NECTA)",
    "338", "Combined New England City and Town Area-New England City and Town Area (NECTA)-State",
    "340", "State-Combined Statistical Area (CSA)",
    "341", "State-Combined Statistical Area (CSA)-Core Based Statistical Area (CBSA)",
    "345", "State-Combined New England City and Town Area",
    "346", "State-Combined New England City and Town Area-New England City and Town Area",
    "350", "New England City and Town Area (NECTA)",
    "351", "New England City and Town Area (NECTA)-State",
    "352", "New England City and Town Area (NECTA)-State-Principal City",
    "353", "New England City and Town Area (NECTA)-State-County",
    "354", "New England City and Town Area (NECTA)-State-County-County Subdivision",
    "355", "New England City and Town Area (NECTA)-NECTA Division",
    "356", "New England City and Town Area (NECTA)-NECTA Division-State",
    "357", "New England City and Town Area (NECTA)-NECTA Division-State-County",
    "358", "New England City and Town Area (NECTA)-NECTA Division-State-County-County Subdivision",
    "360", "State-New England City and Town Area (NECTA)",
    "361", "State-New England City and Town Area (NECTA)-Principal City",
    "362", "State-New England City and Town Area (NECTA)-County",
    "363", "State-New England City and Town Area (NECTA)-County-County Subdivision",
    "364", "State-New England City and Town Area (NECTA)-NECTA Division",
    "365", "State-New England City and Town Area (NECTA)-NECTA Division-County",
    "366", "State-New England City and Town Area (NECTA)-NECTA Division-County-County Subdivision",
    "370", "New England County Metropolitan Area",
    "371", "New England County Metropolitan Area-State",
    "372", "New England County Metropolitan Area-State-Central City",
    "373", "New England County Metropolitan Area-State-County",
    "374", "State-New England County Metropolitan Area",
    "375", "State-New England County Metropolitan Area-Central City",
    "376", "State-New England County Metropolitan Area-County",
    "380", "Metropolitan Statistical Area (MSA)/Consolidated Metropolitan Statistical Area (CMSA)",
    "381", "Metropolitan Statistical Area (MSA)/Consolidated Metropolitan Statistical Area (CMSA)-State",
    "382", "Metropolitan Statistical Area (MSA)/Consolidated Metropolitan Statistical Area (CMSA)-State-Central City",
    "383", "Metropolitan Statistical Area (MSA)/Consolidated Metropolitan Statistical Area (CMSA)-State-County",
    "384",
    "Metropolitan Statistical Area (MSA)/Consolidated Metropolitan Statistical Area (CMSA)-State (New England only)-County-County Subdivision",
    "385", "Consolidated Metropolitan Statistical Area (CMSA)-Primary Metropolitan Statistical Area",
    "386", "Consolidated Metropolitan Statistical Area (CMSA)-Primary Metropolitan Statistical Area-State",
    "387", "Consolidated Metropolitan Statistical Area (CMSA)-Primary Metropolitan Statistical Area-State-County",
    "388",
    "Consolidated Metropolitan Statistical Area (CMSA)-Primary Metropolitan Statistical Area-State (New England only)-County-County Subdivision",
    "390", "State-Metropolitan Statistical Area (MSA)/Consolidated Metropolitan Statistical Area (CMSA)",
    "391", "State-Metropolitan Statistical Area (MSA)/Consolidated Metropolitan Statistical Area (CMSA)-Central City",
    "392", "State-Metropolitan Statistical Area (MSA)/Consolidated Metropolitan Statistical Area (CMSA)-County",
    "393",
    "State (New England only)-Metropolitan Statistical Area (MSA)/Consolidated Metropolitan Statistical Area (CMSA)-County-County Subdivision",
    "395", "State-Consolidated Metropolitan Statistical Area (CMSA)-Primary Metropolitan Statistical Area",
    "396", "State-Consolidated Metropolitan Statistical Area (CMSA)-Primary Metropolitan Statistical Area-County",
    "397",
    "State (New England only)-Consolidated Metropolitan Statistical Area (CMSA)-Primary Metropolitan Statistical Area-County-County Subdivision",
    "400", "Urban Area",
    "410", "Urban Area-State",
    "420", "State-Urban Area",
    "430", "Urban Area-State-County",
    "431", "State-Urban Area-County",
    "440", "Urban Area-State-County-County Subdivision",
    "441", "State-Urban Area-County-County Subdivision",
    "450", "Urban Area-State-County-County Subdivision-Place/Remainder",
    "451", "State-Urban Area-County-County Subdivision-Place/Remainder",
    "460", "Urban Area-State-Central Place",
    "461", "State-Urban Area-Central Place",
    "462", "Urban Area-State-Consolidated City",
    "463", "State-Urban Area-Consolidated City",
    "464", "Urban Area-State-Consolidated City-Place Within Consolidated City",
    "465", "State-Urban Area-Consolidated City-Place Within Consolidated City",
    "500", "State-Congressional District",
    "510", "State-Congressional District-County",
    "511", "State-Congressional District-County-Census Tract",
    "521", "State-Congressional District-County-County Subdivision",
    "531", "State-Congressional District-Place/Remainder",
    "541", "State-Congressional District-Consolidated City",
    "542", "State-Congressional District-Consolidated City-Place Within Consolidated City",
    "550", "State-Congressional District-American Indian Area/Alaska Native Area/Hawaiian Home Land",
    "551",
    "State-Congressional District-American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)",
    "552", "State-Congressional District-American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land",
    "553",
    "State-Congressional District-American Indian Area/Alaska Native Area/Hawaiian Home Land-Tribal Subdivision/Remainder",
    "554",
    "State-Congressional District-American Indian Area/Alaska Native Area (Reservation or Statistical Entity Only)-Tribal Subdivision/Remainder",
    "555",
    "State-Congressional District-American Indian Area (Off-Reservation Trust Land Only)/Hawaiian Home Land-Tribal Subdivision/Remainder",
    "560", "State-Congressional District-Alaska Native Regional Corporation",
    "610", "State Senate District",
    "612", "State Senate District-County",
    "613", "State Senate District-County-Minor Civil Division (MCD)-Place",
    "614", "State Senate District-Place",
    "620", "State House District",
    "622", "State House District-County",
    "623", "State House District-County-Minor Civil Division (MCD)-Place",
    "624", "State House District-Place",
    "700", "Voting Tabulation District (VTD)",
    "740", "Block Group [split by Voting Tabulation District (VTD), Minor Civil Division (MCD), and Place]",
    "750", "Census Block (pl94 files)",
    "795", "State-Public Use Microdata Sample Area (PUMA)",
    "850", "3-digit ZIP Code Tabulation Area (ZCTA3)",
    "851", "State-3-digit ZIP Code Tabulation Area (ZCTA3)",
    "852", "State-3-digit ZIP Code Tabulation Area (ZCTA3)-County",
    "860", "5-digit ZIP Code Tabulation Area (ZCTA5)",
    "870", "5-digit ZIP Code Tabulation Area (ZCTA5)-State",
    "871", "State-5-digit ZIP Code Tabulation Area (ZCTA5)",
    "880", "5-digit ZIP Code Tabulation Area (ZCTA5)-County",
    "881", "State-5-digit ZIP Code Tabulation Area (ZCTA5)-County",
    "901", "County Set",
    "930", "Metropolitan Planning Organization Region (CTPP)",
    "935", "State-County-Combined Zone (CTPP)",
    "940", "State-County-Traffic Analysis Zone (CTPP)",
    "950", "State-School District (Elementary)",
    "960", "State-School District (Secondary)",
    "970", "State-School District (Unified)",
}
