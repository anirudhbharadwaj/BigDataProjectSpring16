from __future__ import print_function

import sys
from operator import add
from polygon import *
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


def simplePolygonTest(l):
	val=l[1]
	polygons=val[0]
	pts=val[1]
	ret=[]
	for poly in polygons:
		polygon=placeDict.value[str(poly)]
		for loc in pts:
			coord,count=loc.strip().split(":")
			latLng=coord.strip().split(";")
			pt=Point(float(latLng[0]),float(latLng[1]))
			if polygon.contains(pt):
				ret.append(str(poly)+";"+str(float(count)/len(polygons)))
	return ret
		
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
	id=getPlaceId(l)
	myList=[]
	xminHash=getHashVal(0,float(bound[0]))
	yminHash=getHashVal(1,float(bound[1]))
	xmaxHash=getHashVal(0,float(bound[2]))
	ymaxHash=getHashVal(1,float(bound[3]))
	for i in range(xminHash,xmaxHash+1):
		for j in range(yminHash,ymaxHash+1):
			myList.append(str(i)+";"+str(j)+"::"+id)
	return myList

def getPlaceId(l):
	line=l.strip().split(",")
	return str(line[0].strip())
	
def getPolygonCoord(l):
	line=l.strip().split(",")
	vertices=[]
	temp=line[6].strip().split(";")
	first=Point(float(temp[1].strip()),float(temp[0].strip()))
	for index in range(6,len(line)):
		pt=line[index].strip().split(";")
		x=float(pt[1].strip())
		y=float(pt[0].strip())
		vertex=Point(x,y)
		vertices.append(vertex)
	vertices.append(first)
	poly=Polygon(vertices)
	return poly

def getPlaceName(l):
	line=l.strip().split(",")
	return str(line[1].strip())+","+str(line[2].strip())+","+str(line[3].strip())+","+str(line[4].strip())

def getFinalVal(l):
	val=l[1]
	name=val[0].strip()
	count=int(val[1])
	return name+","+str(count)
	
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
	sc.addPyFile("/home/abm491/project/polygon.py")
	tb = sc.accumulator([180.0,90.0,-180.0,-90.0], VectorAccumulatorParam())
	c1=taxiData.count()
	#clean the places file
	placeData=placeData.filter(filterPlaces)
	#get the total bounds for filtering points
	placeData.foreach(getTotalBounds)
	pD=placeData.map(lambda l: (getPlaceId(l), getPolygonCoord(l))).collectAsMap()
	placeDict=sc.broadcast(pD)
	#create a rdd with key a serial_no and value as name,category,coordinates
	placeName=placeData.map(lambda l: (getPlaceId(l), getPlaceName(l)))
	totalBound=tb.value
	#filter crop the destination points
	taxiData=taxiData.filter(filterPoints).distinct().map(lambda x: (cropCoordKey(x), cropCoordVal(x))).reduceByKey(add)
	totalPoints=taxiData.count()
	#constants for the dynamic grid index
	c=int(totalPoints**0.5)
	lngV=(totalBound[2]-totalBound[0])/c
	latV=(totalBound[3]-totalBound[1])/c
	#hash the destination points to grid
	taxiData=taxiData.map(lambda x: (hashDest(x), hashDestVal(x))).groupByKey().map(lambda x : (x[0], list(x[1])))
	#hash and list all the grids under each place-polygon's bounds
	placeHashData=placeData.flatMap(getPlaceBounds).map(lambda x: (x.strip().split("::")[0].strip(), x.strip().split("::")[1].strip())).groupByKey().map(lambda x : (x[0], list(x[1])))
	#join the list of places and destination points under each grid
	joinedData=placeHashData.join(taxiData)
	#check if points are present in a polygon and output a <key=serial_no value=count> pair
	countData=joinedData.flatMap(simplePolygonTest).map(lambda x: (x.strip().split(";")[0].strip(), float(x.strip().split(";")[1].strip()))).reduceByKey(add)
	#produce the final rdd to be written in file as 5 columns per row --> category,name,latitude,longitude,count
	nameJoinData=placeName.join(countData).map(getFinalVal).collect()
	for word in nameJoinData:
		print("%s" % (word.encode('utf-8')))
	#Stop Spark
	sc.stop()