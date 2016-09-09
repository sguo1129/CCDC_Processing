#!/alt/local/bin python

'''
 Converting Zhe's main_classificationPar1-2.m version of classification MatLAB code to Python.
 Date: 6/06/2016
 Author: Devendra Dahal, EROS, USGS
 Version 0.0
 Based on: 
	 "
	 CCDC 1.4 version - Zhe Zhu, EROS, USGS "	
Usage: CCDC_main_classification_v1-2.py -help 
'''
 
import os, sys, traceback, time, subprocess, cPickle,scipy.io
import numpy as np
from datetime import date
import datetime as datetime
from optparse import OptionParser 
from sklearn.ensemble import RandomForestClassifier
from scipy.stats import mode
from sklearn.ensemble.forest import _partition_estimators, _parallel_helper
from sklearn.tree._tree import DTYPE
from sklearn.externals.joblib import Parallel, delayed
from sklearn.utils import check_array
from sklearn.utils.validation import check_is_fitted
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

def predict_majvote(forest, X):
	"""Predict class for X.

	Uses majority voting, rather than the soft voting scheme
	used by RandomForestClassifier.predict.

	Parameters
	----------
	X : array-like or sparse matrix of shape = [n_samples, n_features]
		The input samples. Internally, it will be converted to
		``dtype=np.float32`` and if a sparse matrix is provided
		to a sparse ``csr_matrix``.
	Returns
	-------
	y : array of shape = [n_samples] or [n_samples, n_outputs]
		The predicted classes.
	"""
	check_is_fitted(forest, 'n_outputs_')

	# Check data
	X = check_array(X, dtype=DTYPE, accept_sparse="csr")
	print DTYPE
	# Assign chunk of trees to jobs
	n_jobs, n_trees, starts = _partition_estimators(forest.n_estimators,
													forest.n_jobs)

	# Parallel loop
	all_preds = Parallel(n_jobs=n_jobs, verbose=forest.verbose,
						 backend="threading")(
		delayed(_parallel_helper)(e, 'predict', X, check_input=False)
		for e in forest.estimators_)

	# Reduce
	modes, counts = mode(all_preds, axis=0)
	
	if forest.n_outputs_ == 1:
		return forest.classes_.take(modes[0][0]), counts
	else:
		n_samples = all_preds[0].shape[0]
		preds = np.zeros((n_samples, forest.n_outputs_),
						 dtype=forest.classes_.dtype)
		for k in range(forest.n_outputs_):
			preds[:, k] = forest.classes_[k].take(modes[:, k])
		# print len(preds)
		# print preds
		return preds, counts
		
def GetGeoInfo(SourceDS):
	# NDV 		= SourceDS.GetRasterBand(1).GetNoDataValue()
	cols 		= SourceDS.RasterXSize
	rows 		= SourceDS.RasterYSize
	bands	 	= SourceDS.RasterCount
	# GeoT 		= SourceDS.GetGeoTransform()
	# proj 		= SourceDS.GetProjection()
	# extent		= GetExtent(GeoT, cols, rows)
	
	return cols, rows, bands

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
	
def blockshaped(arr, nrows, ncols):
	"""
	Return an array of shape (n, nrows, ncols) where
	n * nrows * ncols = arr.size

	If arr is a 2D array, the returned array looks like n subblocks with
	each subblock preserving the "physical" layout of arr.
	"""
	h, w = arr.shape
	return (arr.reshape(h//nrows, nrows, -1, ncols)
			   .swapaxes(1,2)
			   .reshape(-1, nrows, ncols))	

def Class_Line(Datafile,modelRF,num_c,nbands,anc,ntrees):

	rec_cg = scipy.io.loadmat(Datafile)# scipy use
	rec_cg = rec_cg['rec_cg']
	num_ts = rec_cg.shape[1]
	# print rec_cg[0,1]
	## Matrix of each components
	t_start = rec_cg['t_start']
	t_start = np.concatenate(t_start.reshape(-1).tolist())
	# t_start = (np.concatenate(t_start.reshape(-1).tolist()).reshape(t_start.shape)).tolist()[0]

	t_end = rec_cg['t_end']
	t_end = np.concatenate(t_end.reshape(-1).tolist())	
	# t_end = (np.concatenate(t_end.reshape(-1).tolist()).reshape(t_end.shape)).tolist()[0]
	
	# coefs = rec_cg['coefs']
	rmse = rec_cg['rmse']
	pos = rec_cg['pos']
	# print pos.shape
	# pos = np.concatenate(pos.reshape(-1).tolist())
	pos = (np.concatenate(pos.reshape(-1).tolist()).reshape(pos.shape))[-1]

	IDS_rec = np.zeros(shape=(1,num_ts))
	
	# print IDS_rec.shape
	for i_model in range(0,num_ts):
	
		# print rec_cg[0,i_model]['pos'],
		if not rec_cg[0,i_model]['pos'] == True:
			IDS_rec[0,i_model] = 1
			
	##number of valid curves per line
	num_s = np.size(IDS_rec)
	# print 'num_s', num_s

	##number of bands for ancillary data
	n_anc = np.size(anc, axis=2)	

	##has more than one curves exist for each line
	if num_s > 0:
		## model coefficients
		tmp = rec_cg['coefs']
		# print tmp.shape
		##prepare for classification inputs
		Xclass = np.zeros(shape = (num_s,(num_c+1)*(nbands-1) + n_anc))
		
		## array for storing ancillary data
		array_anc = np.zeros(shape = (n_anc,num_s))
		
		for i_a in range(0,n_anc):
			tmp_anc = anc[:,:,i_a]
			tmp_anc = np.array(np.transpose(tmp_anc).reshape(-1).tolist())
			# print tmp_anc[pos]
			array_anc[i_a,:] = tmp_anc[pos]  
		
		# print'what is this?', rmse.shape
		rmse = np.transpose(np.array(rmse.tolist()[0]),(2,0,1))[0] 
		# print'what is this?', tmp.shape
		tmp1 = np.array(tmp.tolist()[0]) #),(0,1,2))

		# rms1 =  np.array(np.transpose(rmse[0:5]).tolist())

		for icol in range(0,num_s):
			# if icol ==3:
				# sys.exit()
			# print icol, #icol+1, '<-py mat->',((icol-1)*(nbands-1)+1),(nbands-1)*icol
			
			## coefficients from the 7 bands				
			i_tmp = tmp1[icol:,][0]
			# print i_tmp
			## modified constant as inputs
			# i_tmp[:] = i_tmp[:]+(t_start[icol]+ t_end[icol])*i_tmp[:]/2
			
			i_tmp = np.array(np.transpose(i_tmp[:]).reshape(-1).tolist())
			arr_anc = np.array(np.transpose(array_anc[:,icol]).tolist())
			# print '\narr_anc',arr_anc
			rms = np.array(rmse[icol].tolist())	
			# print '\nrms', rms
			# rms = np.array(rmse[(icol-1)*(nbands-1):,icol*(nbands-1)].tolist())			

			## input ready
			# Xclass[icol,:] = np.arange(rmse[((icol-1)*(nbands-1)+1),(nbands-1)*icol],nbands-1,1)).reshape(i_tmp[:],array_anc[:,icol])
			Xclass[icol,:] = np.hstack(([rms],[i_tmp],[arr_anc])) 
		
		with open(modelRF, 'rb') as f:
			rf = cPickle.load(f)
		# rf = np.load(modelRF)	
		# [vote, maps] = predict_majvote(rf,Xclass)
		map = rf.predict(Xclass)
		votes = rf.predict_proba(Xclass)

	## add a new component "class" to rec_cg
	if np.count_nonzero(IDS_rec == 0) > 0:
		IDs_add = np.where(IDS_rec == 0)
		# print len(IDs_add)
		for i in range(0,len(IDs_add)-1):
			np.place(rec_cg[IDs_add][i]['class'], rec_cg[IDs_add][i]['class']>=0,-1)
			np.place(rec_cg[IDs_add][i]['classQA'], rec_cg[IDs_add][i]['classQA']>=0,-1)

	if np.count_nonzero(IDS_rec == 1) > 0:		
		IDs_add = np.where(IDS_rec == 1)
		# print len(IDs_add)
		for i in range(0,len(IDs_add)):

			np.place(rec_cg['class'][IDs_add][i], rec_cg['class'][IDs_add][i]>=0, map[i])

			## largest number of votes
			max_id = np.argmax(votes[i,:], axis=None)
			max_v1 = np.nanmax(votes[i,:], axis=None)
			
			## make this smallest
			votes[i,max_id] = 0

			## second largest number of votes
			max_v2 = np.nanmax(votes[i,:], axis=None)
			
			## provide unsupervised ensemble margin as QA
			np.place(rec_cg['classQA'][IDs_add][i], rec_cg['classQA'][IDs_add][i]>=0, 100*(max_v1-max_v2))

	scipy.io.savemat(Datafile,{'rec_cg':rec_cg})
	# sys.exit()

def allCalc(FileDir, num_c, nbands, ntrees, task, ntasks):
	try:
		
		LogF = FileDir +os.sep+ 'CCDC_Classification_log.txt'	
		f = open(LogF, 'a')	
				
		num_c = int(num_c)
		nbands = int(nbands)
		ntrees = int(ntrees)
		task = int(task)
		ntasks = int(ntasks)
		
		### Writing to log file 
		f.write('Image location =' + FileDir + '\r\n')
		f.write('Number of trees =' + str(ntrees) + '\r\n')
		f.write('************************************\r\n')
		'''
		Start selecting input layers that are required 
		'''
		## selecting training data (this is Land cover trends saved as example_img)
		im_roi 		= FileDir + os.sep + 'example_img'
		
		img_roi = gdal.Open(im_roi)
		ncols, nrows, bands = GetGeoInfo(img_roi) 
		# tim_roi = Arr_trans(im_roi,ncols,nrows,1)
		modelRF 	= FileDir + os.sep + 'modelRF_py.dump'
		
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
		
		## converting all raster to np array				
		asp = Arr_trans(im_aspect,ncols,nrows,1)
		# print anc[0:3]
		slp = Arr_trans(im_slope, ncols,nrows,1)
		dem = Arr_trans(im_dem, ncols,nrows,1)
		pdex = Arr_trans(im_posidex, ncols,nrows,1)
		wpi = Arr_trans(im_wpi, ncols,nrows,1)
		
		im_fmask 	= AncFolder + os.sep + 'Fmask_stat'
		## selectig water, snow, and cloud layer from fmask and converting to array
		wtr = Arr_trans(im_fmask,ncols,nrows,2)
		snw = Arr_trans(im_fmask,ncols,nrows,3)
		cld = Arr_trans(im_fmask,ncols,nrows,4)
		
		anc = np.dstack([asp,slp,dem,pdex,wpi,wtr,snw,cld])
		asp		= None
		slp 	= None
		dem 	= None
		pdex 	= None
		wpi 	= None
		wtr 	= None
		snw 	= None
		cld 	= None
		# if task == 1:
		# print anc[0:3]
		# print 'anc layer done'
		n_result = FileDir + os.sep + 'TSFitMap'
		if not os.path.exists(n_result):
			print 'Folder "%s" with ancillary data could not find.' %n_result 
			os.mkdir(n_result)
			
		# irows = np.zeros(shape=(1,1))
		i = 1
		irows = np.array(int(task) + int(ntasks)*0)
		while int(task) + int(ntasks)*i <= int(nrows):
			new_A = np.array(int(task) + int(ntasks)*i)
			irows = np.append(irows,new_A)		
			i+=1
		# print irows
		size_irows = np.size(irows,0)

		for j in range(1,size_irows+1):
			Datafile = n_result + os.sep + 'record_change'+str(j)+'.mat'
			if not os.path.exists(Datafile):
				print 'Missing the %sth row!' %j
				sys.exit(0)
			else:
				print 'Processing the %sth row!' %j
			Class_Line(Datafile,modelRF,num_c,nbands,anc,ntrees)
			f.write('Row' + str(j) + 'completed at'+tt.strftime("%Y-%m-%d %H:%M:%S")+ '\r\n')	
		
	except:
		print traceback.format_exc()
		
def main():
	parser = OptionParser()

   # define options
	parser.add_option("-i", dest="in_Folder", help="(Required) Location of input data and place to save output")
	parser.add_option("-c", dest="num_coefs",default = 8, help="number of coefficient, default is 8")
	parser.add_option("-b", dest="num_bands", default = 8, help="number of bands, default is 8")
	parser.add_option("-t", dest="ntrees", default = 500, help="number of trees, default is 500")
	parser.add_option("--ts", dest="task",default = 1, help="number of cores to use, default is 1")
	parser.add_option("-n", dest="ntasks", default = 1, help="total number of cores available, default is 1")
	
	
	(ops, arg) = parser.parse_args()

	if len(arg) == 1:
		parser.print_help()
	elif not ops.in_Folder:
		parser.print_help()
		sys.exit(1)
	# elif not ops.gt_start:
		# parser.print_help()
		# sys.exit(1)
	# elif not ops.gt_end:
		# parser.print_help()
		# sys.exit(1)

	else:
		allCalc(ops.in_Folder, ops.num_coefs, ops.num_bands, ops.ntrees, ops.task, ops.ntasks)  
		
		
if __name__ == '__main__':

	main()	

	
t2 = datetime.datetime.now()
print t2.strftime("%Y-%m-%d %H:%M:%S")
tt = t2 - t1
f.write('Processing completed:' + str(tt) + '\r\n')
f.close()
print "\nProcessing time: " + str(tt) 
