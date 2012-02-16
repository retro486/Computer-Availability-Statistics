'''
Last modified 11:18 AM 1/25/2012

@author: rgbernha@nps.edu
@copyright: Naval Postgraduate School
'''

import sys
from datetime import date,datetime,timedelta
import time

#######################
# Settings
#######################
# must match the format of the timestamp in the exported state change CSV:
timestamp_f = '%m/%d/%Y %H:%M'
# sometimes the format below is used. not sure why it flip-flops
#timestamp_f = '%Y-%m-%d %H:%M:%S'

# what date format to use for output
date_f = '%Y-%m-%d'

# a tuple of our normal operating hours, starting with Monday in 24H format
# note for "extended hours" you will need to modify these and re-process the
# PRE-filtered CSV source file (only feed this script affected dates)
# this tuple is used to filter out state changes that occur outside these time slots,
# since we use Deepfreeze to allow for over-night updates, the computers' states appear
# to change even though we are closed...
use_schedule = (
    ('0700','2200'),
    ('0700','2200'),
    ('0700','2200'),
    ('0700','2200'),
    ('0700','1700'),
    ('0900','1700'),
    ('1200','2000'),
)
earliest = 6
latest = 23

# difference tolerance in minutes for open-close times; sometimes clocks don't line up
# and/or someone may open the library early, so allow for some number of minutes
# to be counted early before we open and late before we close
use_tolerance = timedelta(minutes=10) # our clocks seem to vary greatly, so be liberal.

# output files for each tracking dictionary
output_comp_usage = 'output-computer-usage-per-day.csv'
output_peak_hours = 'output-peak-hours-per-day.csv'

##################
# end settings
##################

try:
    f_name = sys.argv[1]
except:
    print 'No filename was specified.'
    sys.exit()

try:
    in_f = open(f_name)
except:
    print 'Unable to open %s for reading.' % f_name
    sys.exit

'''
Sample dictionary structures:

comp_usage = {
    '2000-01-01': {
        'it01400': 14, # on 1/1/2000, it01400 was used for 14 minutes
        ...
    },
    ...
}

peak_hours = {
    '0700': {
        '2000-01-01': 4, # at 0700 on 1/1/2000, there were 4 machines in use
        ...
    },
    ...
}
'''
comps_usage = {}
peak_hours = {}

# keeps track of opened sessions waiting to be closed; for determining session length:
start_holder = {}

lines = in_f.readlines()
in_f.close()

# track earliest day and latest day for ranges; assume the export CSV is a single month
first_day = None
last_day = None
earliest_hour = None
latest_hour = None

# track all computers involved
comp_list = []

# initialize peak hours count
for h in range(earliest,latest):
    hr = str(h) + '00'
    if h < 10:
        hr = '0' + hr
        
    peak_hours[hr] = {}

# line = 'ittag,timestamp,available/unavailable\n'
# data = ('ittag','timestamp','available/unavailable\n')
for line in lines:
    data = line.split(',')
    data[2] = data[2].strip()
    
    # set various datetime objects up
    d = datetime.fromtimestamp(time.mktime(time.strptime(data[1], timestamp_f)))
    dt_open = datetime.strptime( use_schedule[d.weekday()][0], '%H%M' ) - use_tolerance
    dt_close = datetime.strptime( use_schedule[d.weekday()][1], '%H%M' ) + use_tolerance
    # make dates match since we only look at times
    tdt = d - dt_open
    tdt -= timedelta(seconds=tdt.seconds,microseconds=tdt.microseconds)
    dt_open += tdt
    dt_close += tdt
    
    if not (d >= dt_open and d <= dt_close):
        continue # skip this line since it doesn't occur in our "open" hours

    if not data[0] in comp_list:
        comp_list.append(data[0])
        
    ds = d.strftime(date_f)
    
    if first_day is None:
        first_day = d.date()
    elif first_day > d.date():
        first_day = d.date()
        
    if last_day is None:
        last_day = d.date()
    elif last_day < d.date():
        last_day = d.date()
    
    if earliest_hour is None:
        earliest_hour = d.hour
    elif earliest_hour > d.hour:
        earliest_hour = d.hour
        
    if latest_hour is None:
        latest_hour = d.hour
    elif latest_hour < d.hour:
        latest_hour = d.hour
    
    if not comps_usage.has_key(ds):
        comps_usage[ds] = {}

    if not start_holder.has_key(ds):
        start_holder[ds] = {}
        
    if data[2].lower() == 'unavailable':
        # detected start of a new session
        # note that there may be multiple sessions started in a single day on any single
        # computer, depending on how the export CSV is organized, so we have to track
        # multiple sessions per day per computer...
        start_holder[ds][data[0]] = d # store the full timestamp
           
    elif start_holder[ds].has_key(data[0]):
        # end of a session was detected; follow a FIFO stack order for ending sessions
        session_length = d - start_holder[ds][data[0]]
        start_holder[ds].pop(data[0]) # remove this session from the holder
        
        # initialize computer usage tracking for this machine
        if not data[0] in comps_usage[ds]:
            comps_usage[ds][data[0]] = 0
        
        # increment the session counter for this machine on the appropriate date
        comps_usage[ds][data[0]] += (session_length.seconds / 60)
        
        # increment each hour between the start of the session and the end for the same date
        end = d.time().hour
        if d.time().minute > 29:
            end += 1
            
        tst = (d - session_length).time()
        start = tst.hour
        if tst.minute > 29:
            start += 1

        # detect short sessions and just assign them to the start hour
        if start == end:
            hr = str(start) + '00'
            if start < 10:
                hr = '0' + hr
                
            if not peak_hours[hr].has_key(ds):
                peak_hours[hr][ds] = 0
                
            # increment the appropriate peak hour counter
            peak_hours[hr][ds] += 1
        else: # normal sessions that are over an hour long
            for h in range(start,end):
                hr = str(h) + '00'
                if h < 10:
                    hr = '0' + hr
                    
                if not peak_hours[hr].has_key(ds):
                    peak_hours[hr][ds] = 0
                    
                # increment the appropriate peak hour counter
                peak_hours[hr][ds] += 1
            
    else:
        pass
        #print 'Detected end of session that had no start for computer %s on %s.' % (data[0],data[1])
        
try:
    comp_usage_out = open(output_comp_usage,'w')
    peak_hours_out = open(output_peak_hours,'w')
except:
    print 'Unable to open %s or %s for writing. They may be in use.' % (output_comp_usage,output_peak_hours)
    sys.exit(-1)

# write empty leading cell to both output CSV tables
comp_usage_out.write(',')
peak_hours_out.write(',')

# write the header row for each output CSV table
buffer = ''
for comp in comp_list:
    buffer += comp + ','

buffer = buffer[:-1]
comp_usage_out.write('%s\n' % buffer)

buffer = ''
for day in range(first_day.day,last_day.day):
    d = datetime.strptime('%s-%s-%s' % (first_day.year,first_day.month,day), '%Y-%m-%d')
    buffer += str(d.date()) + ','
    
buffer = buffer[:-1]
peak_hours_out.write('%s\n' % buffer)

# build the table of computer usage by day, defaulting to '0' if a computer or day doesn't exist.
for day in range(first_day.day,last_day.day):
    # generate correctly-formatted date
    d = datetime.strftime(datetime.strptime('%s-%s-%s' % (first_day.year,first_day.month,day), '%Y-%m-%d'), '%Y-%m-%d')
    comp_usage_out.write(d + ',')
    buffer = ''
    
    for comp in comp_list:
        if comps_usage.has_key(d) and comps_usage[d].has_key(comp):
            buffer += '%i,' % comps_usage[d][comp]
        else:
            buffer += '0,'
        
    buffer = buffer[:-1] # trim trailing comma
    comp_usage_out.write('%s\n' % buffer)

# build the table of peak hours
for hour in range(earliest_hour,latest_hour):
    h = str(hour) + '00'
    if hour < 10:
        h = '0' + h

    peak_hours_out.write(h + ',')
    buffer = ''

    for day in range(first_day.day,last_day.day):
        # generate correctly-formatted date
        d = datetime.strftime(datetime.strptime('%s-%s-%s' % (first_day.year,first_day.month,day), '%Y-%m-%d'), '%Y-%m-%d')
        
        if peak_hours.has_key(h) and peak_hours[h].has_key(d):
            buffer += '%s,' % peak_hours[h][d]
        else:
            buffer += '0,'
    
    buffer = buffer[:-1]
    peak_hours_out.write('%s\n' % buffer)

comp_usage_out.close()
peak_hours_out.close()
print 'Done!'