from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext
from pyspark.accumulators import AccumulatorParam

class VectorAccumulatorParam(AccumulatorParam):
	def zero(self, value):
		return [180.0,90.0,-180.0,-90.0]
	def addInPlace(self, val1, val2):
		val1[0]=min(val1[0],val2[0])
		val1[2]=max(val1[2],val2[2])
		val1[1]=min(val1[1],val2[1])
		val1[3]=max(val1[3],val2[3])
		return val1

#minX,minY,maxX,maxY ;; X is lng & Y is lat
totalBound=[180.0,90.0,-180.0,-90.0]
totalPoints=0
latV=0.0
lngV=0.0

def filterPlaces(l):
	line=l.strip().split(",")
	ret=False
	if line[0].strip()!="Serial" and len(line)<17 and len(line)>8:
		ret=True
	return ret

def getTotalBounds(l):
	global tb
	bound=getBounds(l)
	tb+=bound

def getBounds(l):
	line=l.strip().split(",")
	bound=[180.0,90.0,-180.0,-90.0]
	for index in range(6,len(line)):
		pt=line[index].strip().split(";")
		x=float(pt[1].strip())
		y=float(pt[0].strip())
		bound[0] = min(bound[0],x)
		bound[2] = max(bound[2],x)
		bound[1] = min(bound[1],y)
		bound[3] = max(bound[3],y)
	return bound
	
def getPlaceBounds(l):
	bound=getBounds(l)
	ret=l+"\t"+str(bound[0])+";"+str(bound[1])+";"+str(bound[2])+";"+str(bound[3])
	return ret

def filterPoints(l):
	line=l.strip().split(",")
	ret=False
	if line[0].strip().lower()!="vendorid":
		x=float(line[9].strip())
		y=float(line[10].strip())
		if x>=totalBound[0] and x<=totalBound[2] and y>=totalBound[1] and y<=totalBound[3]:
			if int(line[0].strip())==1 or int(line[0].strip())==2:
				if int(line[3].strip())>0 and int(line[7].strip())>0 and int(line[7].strip())<7 and (str(line[8].strip()).lower()=="y" or str(line[8].strip()).lower()=="n"):
					if int(line[11].strip())>0 and int(line[11].strip())<7 and float(line[18].strip())>=0.0:
						ret=True;
	return ret;

def cropCoordKey(l):
	line=l.strip().split(",")
	x=str(round(float(line[9].strip()),6))
	y=str(round(float(line[10].strip()),6))
	return x+";"+y

def cropCoordVal(l):
	line=l.strip().split(",")
	return int(line[3].strip())
	
def getHashVal(dimension,val):
	vector=1.0
	if dimension==1:
		vector=latV
	elif dimension==0:
		vector=lngV
	return int((val-totalBound[dimension])/vector)
	
def hashDest(l):
	key=str(l[0])
	coord=key.strip().split(";")
	xHash=getHashVal(0,float(coord[0].strip()))
	yHash=getHashVal(1,float(coord[1].strip()))
	return str(xHash)+";"+str(yHash)

def hashDestVal(l):
	key=str(l[0])
	val=str(l[1])
	return key.strip()+":"+val.strip()
	
	
if __name__ == "__main__":
	#Create SparkContext 
	sc = SparkContext(appName="PythonTest")
	#Read first file: course data
	placeData = sc.textFile(sys.argv[1], 1)
	#Read second file: professor data
	taxiData = sc.textFile(sys.argv[2], 1)
	tb = sc.accumulator([180.0,90.0,-180.0,-90.0], VectorAccumulatorParam())
	c1=taxiData.count()
	placeData=placeData.filter(filterPlaces)
	placeData.foreach(getTotalBounds)
	placeData.map(getPlaceBounds)
	print(tb.value)
	totalBound=tb.value
	#tBound=sc.broadcast(totalBound)
	taxiData=taxiData.filter(filterPoints).distinct().map(lambda x: (cropCoordKey(x), cropCoordVal(x))).reduceByKey(add)
	totalPoints=taxiData.count()
	c=int(totalPoints**0.5)
	lngV=(totalBound[2]-totalBound[0])/c
	latV=(totalBound[3]-totalBound[1])/c
	#vX=sc.broadcast(lngV)
	#vY=sc.broadcast(latV)
	taxiData=taxiData.map(lambda x: (hashDest(x), hashDestVal(x))).sortByKey()
	sampleData=taxiData.take(20)
	print(" bounds are %s , %s , %s , %s" % (str(totalBound[0]), str(totalBound[1]),str(totalBound[2]), str(totalBound[3])))
	print(" points reduced from %i to %i" %(c1,totalPoints))
	for (word, count) in sampleData:
		print("%s: %s" % (word.encode('utf-8'), count.encode('utf-8')))
	#Stop Spark
	sc.stop()