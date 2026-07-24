from dtb_iop_raw_elt.ged.diff_pathfiles import build_pathfiles_df


def test_build_pathfiles_df_assigns_sequential_ids_and_full_path(spark):
    df = build_pathfiles_df(spark, ["a.pdf", "b.pdf"], "\\\\server\\share\\GED")
    rows = {r["filename"]: r.asDict() for r in df.collect()}

    assert rows["a.pdf"]["file_id"] == 0
    assert rows["a.pdf"]["filepath_origem"] == "\\\\server\\share\\GED\\a.pdf"
    assert rows["b.pdf"]["file_id"] == 1
    assert rows["b.pdf"]["filepath_origem"] == "\\\\server\\share\\GED\\b.pdf"


def test_build_pathfiles_df_strips_trailing_slash_from_base_path(spark):
    df = build_pathfiles_df(spark, ["a.pdf"], "\\\\server\\share\\GED\\")
    row = df.collect()[0]
    assert row["filepath_origem"] == "\\\\server\\share\\GED\\a.pdf"


def test_build_pathfiles_df_empty_file_list(spark):
    df = build_pathfiles_df(spark, [], "\\\\server\\share\\GED")
    assert df.count() == 0
    assert df.columns == ["file_id", "filename", "filepath_origem"]
