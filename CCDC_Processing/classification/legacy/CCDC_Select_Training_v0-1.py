#!/alt/local/bin python

'''
 Converting Zhe's 4/14/2016 version of training code written in MatLAB to Python.
 This script contains following matlab code: 'main_Select_Train_Trends1_6_part1.m' 
 Date: 7/12/2016
 Author: Devendra Dahal, EROS, USGS
 Version 0.1: based on 'main_Select_Train_Trends1_6_part1.m'
 Version 0.0: based on 'main_Select_Train_Trends1_4_part1.m'
 Based on: 
	 "Function for extracting training data
	 Prepare data for Training_Strategy.m

	 CCDC 1.6 version - Zhe Zhu, EROS, USGS "	
Usage: CCDC_Select_Training_v0-1.py -help 
'''
 
import os, sys, traceback, time,subprocess, cPickle,scipy.io
import numpy as np
from datetime import date
import datetime as datetime
from optparse import OptionParser 
from sklearn.ensemble import RandomForestClassifier 
try:
	from osgeo import gdal
	from osgeo.gdalconst import *
except ImportError:
	import gdal	
	
print sys.version

t1 = datetime.datetime.now()
print t1.strftime("%Y-%m-%d %H:%M:%S\n")

## define GDAL raster format driver that will be used later on
##--------Start-----------
imDriver = gdal.GetDriverByName('ENVI')
imDriver.Register()
##--------End-----------

def GetExtent(gt,cols,rows):
	''' Return list of corner coordinates from a geotransform

		@type gt:   C{tuple/list}
		@param gt: geotransform
		@type cols:   C{int}
		@param cols: number of columns in the dataset
		@type rows:   C{int}
		@param rows: number of rows in the dataset
		@rtype:    C{[float,...,float]}
		@return:   coordinates of each corner
	'''
	ext=[]
	xarr=[0,cols]
	yarr=[0,rows]

	for px in xarr:
		for py in yarr:
			x=gt[0]+(px*gt[1])+(py*gt[2])
			y=gt[3]+(px*gt[4])+(py*gt[5])
			ext.append([x,y])
			# print x,y
		yarr.reverse()
	return ext

def GetGeoInfo(SourceDS):
	# NDV 		= SourceDS.GetRasterBand(1).GetNoDataValue()
	cols 		= SourceDS.RasterXSize
	rows 		= SourceDS.RasterYSize
	bands	 	= SourceDS.RasterCount
	GeoT 		= SourceDS.GetGeoTransform()
	proj 		= SourceDS.GetProjection()
	extent		= GetExtent(GeoT, cols, rows)
	
	return cols, rows, GeoT, proj, bands, extent

'''	
def findID(inFile,cols, rows):
	array_shape = [cols,rows]
	band1 = inFile.GetRasterBand(1)
	Array1 = band1.ReadAsArray(0, 0, cols, rows)
	
	Inds = np.nonzero(Array1>0)
	Inds = Array1[Inds]
	row_id = RowInd(Array1) 
	# NewArray = np.zeros(shape=(rows,cols)).astype(np.uint16)
	NewArray = np.zeros(shape=(rows,cols), dtype = np.uint32)
	cnt = 1
	for i in range(0,cols):
		# cnt+=1
		for j in range(0,rows):
			if Array1[j,i] > 0:
				NewArray[j,i] = np.uint16(cnt)
				cnt+=1

	rec_l = len(Inds)
	# NewArray = None
	inFile = None	
	Inds = None	
	Array1 = None	
	return row_id,rec_l, NewArray
'''	
def Arr_trans(img,cols,rows, b):
	'''
	Reading raster layer and covering to numpy array
	'''
	img_open = gdal.Open(img)
	band1 = img_open.GetRasterBand(b)
	array = band1.ReadAsArray(0, 0, cols, rows)
	# tras_array = np.transpose(array)
	# array = None
	band1 = None
	return array

def RowIndCell(a):
	'''
	Finds id for each row of numpy array
	'''
	sh = a.shape
	# a = a.tolist()
	inds = []
	cnt = 0
	for i in range(0,sh[0]):
		cnt+=1
		for j in range(0,sh[1]):			
			if a[i,j] > 0:
				inds.append(cnt)				
	return inds

def CellInd(a):
	'''
	Finds id for each cell of numpy array
	'''
	a = a.tolist()
	inds = []
	cnt=1
	for j in a:
		if j > 0:
			inds.append(cnt)
		cnt+=1
	return inds

def allCalc(FileDir,s,e,num_c,nBands):

	try:
		gName = os.path.basename(FileDir)
		## converting date system to gregorian calander from regular date (yyyy,mm,yy) that was supplied in command line
		s = s.split('-')
		e = e.split('-')
		gt_start = date.toordinal(date(int(s[0]),int(s[1]),int(s[2])))+366 ## python date num is lagging one year behind\
		gt_end = date.toordinal(date(int(e[0]),int(e[1]),int(e[2])))+366 ## compared to matlab datenum function
		# start = date.fromordinal(gt_start)
		# end = date.fromordinal(gt_end)
		print 'Start & end dates are %d & %d' %(gt_start, gt_end)
		
		'''Start selecting input layers that are required 
		'''
		## selecting training data (this is Land cover trends saved as example_img)
		im_roi 		= FileDir + os.sep + 'example_img'
		
		img_roi = gdal.Open(im_roi)
		ncols, nrows, GeoT, Proj, bands, dExt = GetGeoInfo(img_roi) 
		tim_roi = Arr_trans(im_roi,ncols,nrows,1)
		
		## put all LCT data disturbance & mining into 10
		tim_roi[tim_roi==3] = 10
		tim_roi[tim_roi==4] = 10
		
		## selecting ancillary dataset that came from NLCD
		AncFolder = FileDir + os.sep + 'ANC'
		if not os.path.exists(AncFolder):
			print 'Folder "%s" with ancillary data could not find.' %AncFolder 
			sys.exit()
		
		im_aspect 	= AncFolder + os.sep + 'aspect'
		im_slope	= AncFolder + os.sep + 'slope'
		im_dem 		= AncFolder +  os.sep + 'dem'
		im_posidex	= AncFolder +  os.sep + 'posidex'
		im_wpi		= AncFolder + os.sep + 'mpw'
		
		## converting all raster to numpy array				
		tim_aspect = Arr_trans(im_aspect,ncols,nrows,1)
		tim_slope = Arr_trans(im_slope, ncols,nrows,1)
		tim_dem = Arr_trans(im_dem, ncols,nrows,1)
		tim_posidex = Arr_trans(im_posidex, ncols,nrows,1)
		tim_wpi = Arr_trans(im_wpi, ncols,nrows,1)
		
		
		## selecting ancillary dataset that came from CCDC 
		im_fmask 	= FileDir + os.sep + 'ANC' + os.sep + 'Fmask_stat'
		if not os.path.exists(im_fmask):
			print 'Fmast layer named as "%s" not existed' %im_fmask 
			sys.exit()
		## selectig water, snow, and cloud layer from fmask and converting to array
		tim_water = Arr_trans(im_fmask, ncols,nrows,1)
		tim_snow = Arr_trans(im_fmask,ncols,nrows,2)
		tim_cloud = Arr_trans(im_fmask, ncols,nrows,3)		
		
		
		### creating a layer of roi with masking out zeros and non zero element	
		Inds = np.nonzero(tim_roi>=0)
		Inds = tim_roi[Inds]
		idsfind = CellInd(Inds)
		
		# i_ids = RowInd(tim_roi)
		# print 'range and  lenght i_ids (%d, %d) & %d' %(min(i_id),max(i_id), len(i_id))
		i_ids = RowIndCell(tim_roi)
		# print 'range and  lenght i_ids (%d, %d) & %d' %(min(i_ids),max(i_ids), len(i_ids))
		rec_l = len(i_ids)

		print 'number of roi pixels is %d' % rec_l	

		n_anc = 5+3
		Xcol = (int(num_c)+1)*(int(nBands)-1)+n_anc
		
		filename = FileDir + os.sep + 'X_tmp.dat'
		X = np.memmap(filename, dtype = 'float16', mode = 'w+', shape=(rec_l,Xcol))
		# X = np.zeros(shape=(rec_l,Xcol)).astype(np.int16)
		Y = np.zeros(shape=(rec_l,2), dtype=np.float16)

		i_row = -1
		plusid = 0
		print '\nProcessing line:-',
		for i in range(0,rec_l):

			if int(i_ids[i]) != i_row:
				print '%d;' % int(i_row+1),
				### Reading TSFitMap for ARD rows and loading as python arrays
				Datafile = FileDir + os.sep + 'TSFitMap'+ os.sep + 'record_change'+str(i_ids[i])+'.mat'
				Chng_mat = scipy.io.loadmat(Datafile)# scipy use
				Chng_mat = Chng_mat['rec_cg']
				# print 'what'
				## Matrix of each components
				t_start = Chng_mat['t_start']
				t_start = np.concatenate(t_start.reshape(-1).tolist())
				# t_start = (np.concatenate(t_start.reshape(-1).tolist()).reshape(t_start.shape)).tolist()[0]

				t_end = Chng_mat['t_end']
				t_end = np.concatenate(t_end.reshape(-1).tolist())
				# t_end = (np.concatenate(t_end.reshape(-1).tolist()).reshape(t_end.shape)).tolist()[0]
								
				coefs = Chng_mat['coefs']
				rmse = Chng_mat['rmse']
				pos = Chng_mat['pos']
				pos = (np.concatenate(pos.reshape(-1).tolist()).reshape(pos.shape))[-1]
				
				category = Chng_mat['category']
				category = (np.concatenate(category.reshape(-1).tolist()).reshape(category.shape)).tolist()[0]	

				# coefs = coefs.reshape(int(num_c),int(nBands)-1, rn)
				# print coefs.shape
				# sys.exit()
			ids_line1 = np.where(pos == int(idsfind[i]))[0]
			idsize = ids_line1.size
			if idsize < 1:
				continue
			else:
				ids_line = ids_line1[:idsize]
				# print ids_line.size+1
			for j in range(0, ids_line.size):
				id_ref = ids_line[j]
				
				pos_ref = pos[id_ref]
				
				### take curves that fall witin the training period
				### remove curves that are changed within training period
				if ((t_start[id_ref] < gt_start) & (t_end[id_ref] > gt_end)) | np.logical_and(((tim_roi.item(pos_ref) == 10) & (t_start[id_ref] < gt_end)),(t_end[id_ref] > gt_end)):
					
					tmp_cft = coefs[...,id_ref]

					tmp_cft = np.reshape(np.ravel(tmp_cft[0]), (8, 7))

					tmp_cft[0,:] = tmp_cft[0,:]+ gt_end*tmp_cft[1,:]
					tmp_cft = tmp_cft.T
					tmp_rmse =  rmse[:,id_ref]
					
					all_anc = map(float,[tim_aspect.item(pos_ref), tim_dem.item(pos_ref),tim_posidex.item(pos_ref),
						tim_slope.item(pos_ref),tim_wpi.item(pos_ref),tim_water.item(pos_ref),
						tim_snow.item(pos_ref),tim_cloud.item(pos_ref)])

					tmp_rmse = np.concatenate(tmp_rmse.reshape(-1).tolist())					
					tmp_rmse =  tmp_rmse.reshape(-1).tolist()

					tmp_cft = np.array(tmp_cft.tolist()).reshape(-1).tolist()
					
					# print tmp_cft
					s = tmp_rmse + tmp_cft + all_anc
					# s = [elem[:6] for elem in str(s)]
					# s = [round(float(x),4) for x in s]

					X[plusid,:] = s
					Y[plusid,0] = tim_roi.item(pos_ref)
					Y[plusid,1] = pos_ref
					plusid += 1		
			i_row = i_ids[i]
			
		## remove out of boundary or changed pixels
		print '\nplusid %d' %plusid
		
		X = X[1:plusid,:]
		Y = Y[1:plusid,:]
		# print Y.size
		# print Y.shape
		'''
		Xfile = FileDir + os.sep + 'Xs_'+ gName+'.out'
		Yfile = FileDir + os.sep + 'Ys_'+ gName+'.out'
		
		np.savetxt(Xfile, X, delimiter=',',fmt='%1.6f')   # X is an array
		np.savetxt(Yfile, Y, delimiter=',',fmt='%1.6f')   # X is an array
		'''
		## computing part2 of matlab
		# ComputeRandomForest(X, Y, 100, FileDir)
		
		XfileM = FileDir + os.sep + 'Xs_'+ gName+'.npy'
		YfileM = FileDir + os.sep + 'Ys_'+ gName+'.npy'
		
		np.save(XfileM, X)   # X is an array
		np.save(YfileM, Y)   # Y is an array
		
		X = None
		Y = None
		os.remove(filename)
	except:
		print "Processed halted on the way."
		print traceback.format_exc()
	
def main():
	parser = OptionParser()

   # define options
	parser.add_option("-i", dest="in_Folder", help="(Required) Location of input data and place to save output")
	parser.add_option("-s", dest="gt_start", help="(Required) ground truth start time, example: yyyy-mm-dd")
	parser.add_option("-e", dest="gt_end", help="(Required) ground truth end time, example: yyyy-mm-dd")
	parser.add_option("-c", dest="num_coef",default = 8, help="number of coefficient, default is 8")
	parser.add_option("-b", dest="num_bands",default = 8, help="number of bands, default is 8")
	# parser.add_option("-r", dest="num_rows", help="(Required) number of rows to get image dimension")
	# parser.add_option("-c", dest="num_cols", help="(Required) number of cols to get image dimension")
	(ops, arg) = parser.parse_args()

	if len(arg) == 1:
		parser.print_help()
	elif not ops.in_Folder:
		parser.print_help()
		sys.exit(1)
	elif not ops.gt_start:
		parser.print_help()
		sys.exit(1)
	elif not ops.gt_end:
		parser.print_help()
		sys.exit(1)
	# elif not ops.num_coef:
		# parser.print_help()
		# sys.exit(1)
	# elif not ops.num_bands:
		# parser.print_help()
		# sys.exit(1)
	else:
		allCalc(ops.in_Folder, ops.gt_start, ops.gt_end, ops.num_coef,ops.num_bands)  

if __name__ == '__main__':

	main()
	
t2 = datetime.datetime.now()
print t2.strftime("%Y-%m-%d %H:%M:%S")
tt = t2 - t1
print "\nProcessing time: " + str(tt) 
