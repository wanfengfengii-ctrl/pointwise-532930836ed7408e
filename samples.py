ROWS = {"": b"", "a": b"old", "aa": b"same", "水位": b"12"}
LEFT_CHANGES = {"a": b"new", "水位": None}
RIGHT_CHANGES = {"aa": None, "ab": b"added"}
EXPECTED = [("a", b"new", b"old"), ("aa", b"same", None),
            ("ab", None, b"added"), ("水位", None, b"12")]
