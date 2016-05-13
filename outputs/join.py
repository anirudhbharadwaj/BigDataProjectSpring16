import shutil
import glob

def copyFiles(inputfilenames,outfilename):
	with open(outfilename, 'wb') as outfile:
		for filename in glob.glob(inputfilenames):
			with open(filename, 'rb') as readfile:
				shutil.copyfileobj(readfile, outfile)


for i in range(2010,2016):
	year=str(i)
	copyFiles(year+"\*.txt","yearData\\"+year+".txt")
copyFiles("yearData\*.txt","allData.txt")