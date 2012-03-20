""" Process track data from the million song dataset and create a dictionary
	entry for each song containing all the song properties. All the properties 
	are stored under the respective meaningful dictionary keynames. Each song is
	then stored as a document into the song_db database. The key element for each
	song entry is the 'song_id' represented by the primary key '_id'

	 The total number of features being considered for each song is currently 19. That 
	 number may increase depending on future ideas that come into my head.
	
"""
import sys
sys.path.append('PythonSrc')

import os
import glob
import hdf5_getters
import pymongo
import time

# Global parameters that may need to accessed by different parts of the project
# Contents of the parameters variable is not fixed and may change depending on 
# features and tweaks being implemented
global parameters
parameters = dict(
		dataset_path = sys.argv[1],#'/home/vivek/datasets/MillionSongSubset/data/', 
		summaryfile_path = '/home/vivek/Desktop')
		

''' Connect to the mongoDB called song_db. Make the database name customisable'''
def connect_song_db():
	connection = pymongo.Connection()	
	db = connection['song_db']
	song_db = db.song_db
	return song_db


''' Traverse though all the folders in the MSD directory tree and create an array of all
hd5 filepaths so that contents of the individual file path can be processed'''
def get_h5_files(base_dir, ext='.h5'):
	h5_files = []
	for dirpath, dirname, filenames in os.walk(base_dir):
		for filename in filenames:
			fileext = os.path.splitext(filename)
			if (fileext[1] == ext):
				try:
					h5_files.append(os.path.join(dirpath, filename))
					
				except IOError as e: 
					print os.path.join(dirpath, filename)+' not found'
	return h5_files

'''Compute the variance of all values in the array'''
def variance(array):
	if(len(array) == 0):
		return 0

	m = mean(array)
	sum_of_diff = 0
	for i in range(len(array)):
		sum_of_diff += (array[i] - m)
	v = sum_of_diff**2/len(array)
	return v

'''Compute mean of all values in the array'''
def mean(array):
	if (len(array) == 0):
		return 0

	return sum(array)/len(array)

'''Split a 2D array into 12 columms so that the mean and variance for each component 
of the 12 basis values of the pitch and variance matrix can be calculated'''
def split_segments(array2d):
	a0 = [];a1 = [];a2 = [];a3 = [];a4 = [];a5 = [];a6 = [];a7 = []
	a8 = [];a9 = [];a10 = [];a11 = []
	if (len(array2d) != 0):
		for j in range(len(array2d)):
			a0.append(array2d[j][0])
			a1.append(array2d[j][1])
			a2.append(array2d[j][2])
			a3.append(array2d[j][3])
			a4.append(array2d[j][4])
			a5.append(array2d[j][5])
			a6.append(array2d[j][6])
			a7.append(array2d[j][7])
			a8.append(array2d[j][8])
			a9.append(array2d[j][9])
			a10.append(array2d[j][10])
			a11.append(array2d[j][11])
	return (a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11)

'''This is where each song is processed and the corresponding properties are extracted. This 
function should be pretty much self explanatory. Hope I dont forget what it is supposed to do'''
def process_song(h5_song_file):
	song = {}
	song['artist_familiarity'] = hdf5_getters.get_artist_familiarity(h5)
	song['artist_id'] = hdf5_getters.get_artist_id(h5)
	song['artist_name'] = hdf5_getters.get_artist_name(h5)
	song['artist_hotttnesss'] = hdf5_getters.get_artist_hotttnesss(h5);
	song['title'] = hdf5_getters.get_title(h5)
	terms = hdf5_getters.get_artist_terms(h5)
	terms_freq = hdf5_getters.get_artist_terms_freq(h5)
	terms_weight = hdf5_getters.get_artist_terms_weight(h5)
	terms_array = []
	# Creating a array of [term, its frequency, its weight]. Doing this for all terms associated
	# with the artist
	for i in range(len(terms)):
		terms_array.append([terms[i], terms_freq[i], terms_weight[i]])	
		
	song['artist_terms'] = terms_array
	beats_start = hdf5_getters.get_beats_start(h5)
	song['beats_start_variance'] = variance(beats_start)   #beats variance in yocto seconds(10^-24s)
	song['number_of_beats'] = len(beats_start)
	song['duration'] = hdf5_getters.get_duration(h5)
	song['loudness'] = hdf5_getters.get_loudness(h5)
	sections_start = hdf5_getters.get_sections_start(h5)
	song['sections_start_variance'] = variance(sections_start)
	song['number_of_sections'] = len(sections_start)
	
	segments_pitches = hdf5_getters.get_segments_pitches(h5)
	(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11) = split_segments(segments_pitches)
	song['segments_pitches_variance'] = [variance(a0), variance(a1), variance(a2),
					variance(a3), variance(a4), variance(a5), variance(a6), variance(a7),
					variance(a8), variance(a9), variance(a10), variance(a11)]
	song['segments_pitches_mean'] = [mean(a0), mean(a1), mean(a2), mean(a3), mean(a4), 
					mean(a5), mean(a6), mean(a7), mean(a8), mean(a9), mean(a10), mean(a11)]
	
	segments_timbre = hdf5_getters.get_segments_timbre(h5)
	(a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11) = split_segments(segments_timbre)
	song['segments_timbre_variance'] = [variance(a0), variance(a1), variance(a2),
					variance(a3), variance(a4), variance(a5), variance(a6), variance(a7),
					variance(a8), variance(a9), variance(a10), variance(a11)]
	song['segments_timbre_mean'] = [mean(a0), mean(a1), mean(a2), mean(a3), mean(a4), 
					mean(a5), mean(a6), mean(a7), mean(a8), mean(a9), mean(a10), mean(a11)]
	song['tempo'] = hdf5_getters.get_tempo(h5)
	song['_id'] = hdf5_getters.get_song_id(h5)
	song['year'] = hdf5_getters.get_year(h5)	
	return song


if __name__ == '__main__':
	h5_files = get_h5_files(parameters['dataset_path'])
	song_db = connect_song_db()
	count = 0
	start_clock = time.time()
	for fname in h5_files:
		count += 1
		h5 = hdf5_getters.open_h5_file_read(fname) 
		song = process_song(h5)

		# Insert song to the database
		song_db.insert(song)
		print count
		h5.close()
	end_clock = time.time()
	total_time = end_clock - start_clock
	print ('Done processing songs and adding them to database in {0} seconds', format(total_time))


# blah = songs.find({'number_of_beats':{'$gt':300}})
# for i in blah:
# 	print i


