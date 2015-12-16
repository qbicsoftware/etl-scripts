CODE = "Data Set Code"
PATH = "Path"
NAME = "Name"
SIZE = "Size"

def aggregate(parameters, tableBuilder):
	codes = parameters.get("codes")

	tableBuilder.addHeader(CODE)
	tableBuilder.addHeader(NAME)
	tableBuilder.addHeader(SIZE)
	tableBuilder.addHeader(PATH)

	for code in codes:
		root = contentProvider.getContent(code).getRootNode()
		handleNode(root, code, tableBuilder)

def handleNode(node, dataSetCode, tableBuilder):
	if node.isDirectory():
		for child in node.getChildNodes():
			handleNode(child, dataSetCode, tableBuilder)
	else:
		row = tableBuilder.addRow()
		row.setCell(CODE, dataSetCode)
		row.setCell(NAME, node.getName())
		row.setCell(PATH, node.getRelativePath())
		#row.setCell(LAST_MODIFIED, Date(node.getLastModified()))
		row.setCell(SIZE, node.getFileLength())
