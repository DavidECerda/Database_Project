Table:
  PageRangeSize: 12
  PageRangeBasePages: 4

  NumRecords: 8
  PageRanges = [
    PageRange,
    PageRange,
    PageRange,
  ]
  PageDir: {
    0: [pageRange, pageInRange, cellInPage]
    1: 01
    2: 02
    3: 03
    4: 160
    5:
    6:
    7:
  }

  getRecord(rid) {
    pageAddress = PageDir[rid]
    pageRange = pageAddress[0]
    pageInRange = pageAddress[1]
    cellInPage = pageAddress[2]
    page = PageRanges[pageRange].get_page(pageInRange)
    page.getCell(cellInRange) # this will be some byte memory shit
  }

PageRange:
  Pages = list of size 12
  baseRecordCount = 0
  tailRecords = 0
  createRecord()


  ===

  Table:
  PageRangeSize: 12
  PageRangeBasePages: 4

  NumRecords: 8
  PageRanges = [
    PageRange,
    PageRange,
    PageRange,
  ]
  PageDir: {
    0: [pageRange, pageInRange, cellInPage]
    1: 01
    2: 02
    3: 03
    4: 160
    5:
    6:
    7:
  }

  getRecord(rid) {
    pageAddress = PageDir[rid]
    pageRange = pageAddress[0]
    pageInRange = pageAddress[1]
    cellInPage = pageAddress[2]
    page = PageRanges[pageRange].get_page(pageInRange)
    page.getCell(cellInRange) # this will be some byte memory shit
  }

PageRange:
  Pages = list of size 12
  baseRecordCount = 0
  tailRecords = 0
  createRecord()