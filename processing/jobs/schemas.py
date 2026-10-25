"""Tên và kiểu 61 cột của bảng Events, theo GDELT-Event_Codebook-V2.0.pdf.

File gốc là TSV không có dòng tiêu đề, nên thứ tự trong danh sách này phải đúng tuyệt đối.
Các cột mã (code) giữ kiểu chuỗi để không mất số 0 đứng đầu, ví dụ mã sự kiện "04".
"""

ACTOR_FIELDS = [
    ("code", "string"), ("name", "string"), ("country_code", "string"),
    ("known_group_code", "string"), ("ethnic_code", "string"),
    ("religion1_code", "string"), ("religion2_code", "string"),
    ("type1_code", "string"), ("type2_code", "string"), ("type3_code", "string"),
]

GEO_FIELDS = [
    ("type", "int"), ("full_name", "string"), ("country_code", "string"),
    ("adm1_code", "string"), ("adm2_code", "string"),
    ("lat", "double"), ("long", "double"), ("feature_id", "string"),
]

EVENT_SCHEMA = (
    [
        ("global_event_id", "long"),
        ("sql_date", "string"),        # YYYYMMDD: ngày sự kiện diễn ra
        ("month_year", "int"),
        ("year", "int"),
        ("fraction_date", "double"),
    ]
    + [(f"actor1_{name}", dtype) for name, dtype in ACTOR_FIELDS]
    + [(f"actor2_{name}", dtype) for name, dtype in ACTOR_FIELDS]
    + [
        ("is_root_event", "int"),
        ("event_code", "string"),
        ("event_base_code", "string"),
        ("event_root_code", "string"),
        ("quad_class", "int"),
        ("goldstein_scale", "double"),
        ("num_mentions", "int"),
        ("num_sources", "int"),
        ("num_articles", "int"),
        ("avg_tone", "double"),
    ]
    + [(f"actor1_geo_{name}", dtype) for name, dtype in GEO_FIELDS]
    + [(f"actor2_geo_{name}", dtype) for name, dtype in GEO_FIELDS]
    + [(f"action_geo_{name}", dtype) for name, dtype in GEO_FIELDS]
    + [
        ("date_added", "string"),      # YYYYMMDDHHMMSS: lúc GDELT ghi nhận, độ phân giải 15 phút
        ("source_url", "string"),
    ]
)

assert len(EVENT_SCHEMA) == 61, len(EVENT_SCHEMA)

# 27 cột của GKG 2.1, theo GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf
GKG_COLUMNS = [
    "gkg_record_id", "date", "source_collection_id", "source_common_name", "document_identifier",
    "counts", "v2_counts", "themes", "v2_themes", "locations", "v2_locations",
    "persons", "v2_persons", "organizations", "v2_organizations", "v2_tone",
    "dates", "gcam", "sharing_image", "related_images", "social_image_embeds", "social_video_embeds",
    "quotations", "all_names", "amounts", "translation_info", "extras",
]

assert len(GKG_COLUMNS) == 27, len(GKG_COLUMNS)