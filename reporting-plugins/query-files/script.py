DATA_SOURCE = "path-info-db"
QUERY = """
    SELECT ds.code as "data_set_code", dsf.*
    FROM data_sets ds, data_set_files dsf
    WHERE ds.code = ?{1} AND dsf.dase_id = ds.id
"""
 
"""reporting table column names"""
DATA_SET_CODE = "Data Set"
RELATIVE_PATH = "Relative Path"
FILE_NAME = "File Name"
SIZE_IN_BYTES = "Size"
FOLDER = "Folder"
LAST_MODIFIED = "Last Modified"

def aggregate(parameters, tableBuilder):
    codes = parameters.get("codes") 
    tableBuilder.addHeader(DATA_SET_CODE)
    tableBuilder.addHeader(RELATIVE_PATH)
    tableBuilder.addHeader(FILE_NAME)
    tableBuilder.addHeader(SIZE_IN_BYTES)
    tableBuilder.addHeader(FOLDER)
    tableBuilder.addHeader(LAST_MODIFIED)
 
    for code in codes:
        results = queryService.select(DATA_SOURCE, QUERY, [code])
        for r in results:
            # ignore "original" folder
            if not r.get("RELATIVE_PATH".lower()) in ["original", ""]:
		row = tableBuilder.addRow()
		folder = ""
		splt = r.get("RELATIVE_PATH".lower()).split("/")
		if len(splt) > 1:
			folder = splt[-2]
                row.setCell(DATA_SET_CODE, r.get("DATA_SET_CODE".lower()))
                row.setCell(RELATIVE_PATH, r.get("RELATIVE_PATH".lower()))
                row.setCell(FILE_NAME, r.get("FILE_NAME".lower()))
                row.setCell(SIZE_IN_BYTES, r.get("SIZE_IN_BYTES".lower()))
                row.setCell(FOLDER, folder)
                row.setCell(LAST_MODIFIED, r.get("LAST_MODIFIED".lower()))
        results.close()
