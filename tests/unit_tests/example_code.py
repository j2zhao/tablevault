from tablevault.core import TableVault

tablevault = TableVault(db_dir="test", author="jinjin", create=True)
tablevault.setup_table("short_stories", allow_multiple_artifacts=False)
tablevault.copy_files("../test_data/stories", table_name="short_stories")
tablevault.setup_temp_instance("short_stories", builder_names=["gen_stories"])
