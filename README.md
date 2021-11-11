# utl-loading-a-small_50gb-sas-dataset-into-python-in-under-three-minutes
Loading a small 50gb sas dataset into python in under three minutes  
    %let pgm=utl-loading-a-small_50gb-sas-dataset-into-python-in-under-three-minutes;

    StackOverflow
    https://tinyurl.com/e24jdcea
    https://stackoverflow.com/questions/58612304/reading-huge-sas-dataset-in-python

    github
    https://tinyurl.com/dzxww8be
    https://github.com/rogerjdeangelis/utl-loading-a-small_50gb-sas-dataset-into-python-in-under-three-minutes

    also see
    https://tinyurl.com/yd4esoo5
    https://github.com/rogerjdeangelis/utl-benchmarks-for-python-pyreadstat-vs-pandas-read_sas-for-inporting-sas7bdats

    Loading a small 50gb sas dataset into python in under three minutes

    50gb is only 1/20 of a terabyte. Single tables over 1Tb sre big data.

    This code was run on a $600 Dell 7610 with 128gb of ram, 32 logical processors (16 cpus 2 threads each),
    4 ssds and 2 NVME drives.

    Twelve batch SAS jobs were run in parallel.

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    * Split the SAS dataset into 12 pieces each 4.2GB 50,400,000,000 bytes;

    libname c  "c:/wrk";
    libname d  "d:/wrk";
    libname e  "e:/wrk";
    libname f  "f:/wrk";
    libname g  "g:/wrk";
    libname m  "m:/wrk";
    libname c1 "c:/wrx";
    libname d1 "d:/wrx";
    libname e1 "e:/wrx";
    libname f1 "f:/wrx";
    libname g1 "g:/wrx";
    libname m1 "m:/wrx";

    %array(slc,values= c.have d.have e.have f.have g.have m.have c1.have d1.have e1.have f1.have g1.have m1.have);

    data %do_over(slc,phrase=?);
      set sashelp.class;
      do i=1 to 5500000;
        output;
      end;
      drop i ;
    run;quit;

    /* EACH is $gb
    NOTE: The data set C.HAVE has 104500000 observations and 5 variables. * each 4gb;
    NOTE: The data set D.HAVE has 104500000 observations and 5 variables.
    NOTE: The data set E.HAVE has 104500000 observations and 5 variables.
    NOTE: The data set F.HAVE has 104500000 observations and 5 variables.
    NOTE: The data set G.HAVE has 104500000 observations and 5 variables.
    NOTE: The data set M.HAVE has 104500000 observations and 5 variables.
    NOTE: The data set C1.HAVE has 104500000 observations and 5 variables.
    NOTE: The data set D1.HAVE has 104500000 observations and 5 variables.
    NOTE: The data set E1.HAVE has 104500000 observations and 5 variables.
    NOTE: The data set F1.HAVE has 104500000 observations and 5 variables.
    NOTE: The data set G1.HAVE has 104500000 observations and 5 variables.
    NOTE: The data set M1.HAVE has 104500000 observations and 5 variables.
    */

    /*           _               _
      ___  _   _| |_ _ __  _   _| |_
     / _ \| | | | __| `_ \| | | | __|
    | (_) | |_| | |_| |_) | |_| | |_
     \___/ \__,_|\__| .__/ \__,_|\__|
                    |_|
    */

    The chunksize was 20,000,000 records.

    The complete output is 12 lists each with 6 dataframes for a total of 72 dataframes.

    Since you probably cannot work with the entire 50gb you may want to append the
    dataframes in a smaller set, depending on the size of your mrmory.

    DATAFRAME DF (LIST OF 5 PIECES EACH WITH 20,000,000 RECORDS AND ONE WITH 4,500,000)

                NAME SEX   AGE  HEIGHT  WEIGHT
    0         Alfred   M  14.0    69.0   112.5
    1         Alfred   M  14.0    69.0   112.5
    2         Alfred   M  14.0    69.0   112.5
    3         Alfred   M  14.0    69.0   112.5
    4         Alfred   M  14.0    69.0   112.5
    ...          ...  ..   ...     ...     ...
    19999995   Carol   F  14.0    62.8   102.5
    19999996   Carol   F  14.0    62.8   102.5
    19999997   Carol   F  14.0    62.8   102.5
    19999998   Carol   F  14.0    62.8   102.5
    19999999   Carol   F  14.0    62.8   102.5

    [20000000 rows x 5 columns]
               NAME SEX   AGE  HEIGHT  WEIGHT
    0         Carol   F  14.0    62.8   102.5
    1         Carol   F  14.0    62.8   102.5
    2         Carol   F  14.0    62.8   102.5
    3         Carol   F  14.0    62.8   102.5
    4         Carol   F  14.0    62.8   102.5
    ...         ...  ..   ...     ...     ...
    19999995  Janet   F  15.0    62.5   112.5
    19999996  Janet   F  15.0    62.5   112.5
    19999997  Janet   F  15.0    62.5   112.5
    19999998  Janet   F  15.0    62.5   112.5
    19999999  Janet   F  15.0    62.5   112.5

    [20000000 rows x 5 columns]
               NAME SEX   AGE  HEIGHT  WEIGHT
    0         Janet   F  15.0    62.5   112.5
    1         Janet   F  15.0    62.5   112.5
    2         Janet   F  15.0    62.5   112.5
    3         Janet   F  15.0    62.5   112.5
    4         Janet   F  15.0    62.5   112.5
    ...         ...  ..   ...     ...     ...
    19999995  Joyce   F  11.0    51.3    50.5
    19999996  Joyce   F  11.0    51.3    50.5
    19999997  Joyce   F  11.0    51.3    50.5
    19999998  Joyce   F  11.0    51.3    50.5
    19999999  Joyce   F  11.0    51.3    50.5

    [20000000 rows x 5 columns]
                NAME SEX   AGE  HEIGHT  WEIGHT
    0          Joyce   F  11.0    51.3    50.5
    1          Joyce   F  11.0    51.3    50.5
    2          Joyce   F  11.0    51.3    50.5
    3          Joyce   F  11.0    51.3    50.5
    4          Joyce   F  11.0    51.3    50.5
    ...          ...  ..   ...     ...     ...
    19999995  Philip   M  16.0    72.0   150.0
    19999996  Philip   M  16.0    72.0   150.0
    19999997  Philip   M  16.0    72.0   150.0
    19999998  Philip   M  16.0    72.0   150.0
    19999999  Philip   M  16.0    72.0   150.0

    [20000000 rows x 5 columns]
                 NAME SEX   AGE  HEIGHT  WEIGHT
    0          Philip   M  16.0    72.0   150.0
    1          Philip   M  16.0    72.0   150.0
    2          Philip   M  16.0    72.0   150.0
    3          Philip   M  16.0    72.0   150.0
    4          Philip   M  16.0    72.0   150.0
    ...           ...  ..   ...     ...     ...
    19999995  William   M  15.0    66.5   112.0
    19999996  William   M  15.0    66.5   112.0
    19999997  William   M  15.0    66.5   112.0
    19999998  William   M  15.0    66.5   112.0
    19999999  William   M  15.0    66.5   112.0

    [20000000 rows x 5 columns]
                NAME SEX   AGE  HEIGHT  WEIGHT
    0        William   M  15.0    66.5   112.0
    1        William   M  15.0    66.5   112.0
    2        William   M  15.0    66.5   112.0
    3        William   M  15.0    66.5   112.0
    4        William   M  15.0    66.5   112.0
    ...          ...  ..   ...     ...     ...
    4499995  William   M  15.0    66.5   112.0
    4499996  William   M  15.0    66.5   112.0
    4499997  William   M  15.0    66.5   112.0
    4499998  William   M  15.0    66.5   112.0
    4499999  William   M  15.0    66.5   112.0

    [4500000 rows x 5 columns]

    /*
     _ __  _ __ ___   ___ ___  ___ ___
    | `_ \| `__/ _ \ / __/ _ \/ __/ __|
    | |_) | | | (_) | (_|  __/\__ \__ \
    | .__/|_|  \___/ \___\___||___/___/
    |_|
     _            _     _       _                      _   _           _
    | |_ ___  ___| |_  (_)_ __ | |_ ___ _ __ __ _  ___| |_(_)_   _____| |_   _
    | __/ _ \/ __| __| | | `_ \| __/ _ \ `__/ _` |/ __| __| \ \ / / _ \ | | | |
    | ||  __/\__ \ |_  | | | | | ||  __/ | | (_| | (__| |_| |\ V /  __/ | |_| |
     \__\___||___/\__| |_|_| |_|\__\___|_|  \__,_|\___|\__|_| \_/ \___|_|\__, |
                                                                         |___/
    */

    * COPY THIS MACRO TO YOUR AUTOCALL LIBRARY COMPILE AND TEST INTERACTIVELY;
    * CHANGE c:/oto yo YOUR AUTOCALL LIBRARY;

    filename ft15f001 "c:/oto/utl_slice.sas";
    parmcards4;
    %macro utl_slice(one);
    %utl_submit_py64_38("
    import pyreadstat;
    fpath = '&one/have.sas7bdat';
    df = pyreadstat.read_file_in_chunks(pyreadstat.read_sas7bdat, fpath, chunksize= 20000000);
    for df, meta in df:;
    .    print(df);
    ");
    %mend utl_slice;
    ;;;;
    run;quit;

    * COMPILE AUTOCALL MACRO;

    %inc "c:/oto/utl_slice.sas";

    * RUN;
    %utl_slice(c:/wrx);

    /* ____                          _ _      _     _       _
    / |___ \   _ __   __ _ _ __ __ _| | | ___| |   (_) ___ | |__  ___
    | | __) | | `_ \ / _` | `__/ _` | | |/ _ \ |   | |/ _ \| `_ \/ __|
    | |/ __/  | |_) | (_| | | | (_| | | |  __/ |   | | (_) | |_) \__ \
    |_|_____| | .__/ \__,_|_|  \__,_|_|_|\___|_|  _/ |\___/|_.__/|___/
              |_|                                |__/
    */

    * SAS COMMAND LINE
    %let _s=%sysfunc(compbl(C:\Progra~1\SASHome\SASFoundation\9.4\sas.exe -sysin c:\nul -sasautos c:\oto -rsasuser
    -work f:\wrk));

    options noxwait noxsync;
    %let tym=%sysfunc(time());
    systask kill sys1 sys2 sys3 sys4 sys5 sys6 sys7 sys8 sys9 sys10 sys11 sys12 sys13 sys14 sys15 sys16;
    systask command "&_s -termstmt %nrstr(%utl_slice(c:/wrk);) -log d:\log\a1.log -print d:\log\p1.txt" taskname=sys1;
    systask command "&_s -termstmt %nrstr(%utl_slice(d:/wrk);) -log d:\log\a2.log -print d:\log\p2.txt" taskname=sys2;
    systask command "&_s -termstmt %nrstr(%utl_slice(e:/wrk);) -log d:\log\a3.log -print d:\log\p3.txt" taskname=sys3;
    systask command "&_s -termstmt %nrstr(%utl_slice(f:/wrk);) -log d:\log\a4.log -print d:\log\p4.txt" taskname=sys4;
    systask command "&_s -termstmt %nrstr(%utl_slice(g:/wrk);) -log d:\log\a5.log -print d:\log\p5.txt" taskname=sys5;
    systask command "&_s -termstmt %nrstr(%utl_slice(m:/wrk);) -log d:\log\a6.log -print d:\log\p6.txt" taskname=sys6;
    systask command "&_s -termstmt %nrstr(%utl_slice(c:/wrx);) -log d:\log\a7.log -print d:\log\p7.txt" taskname=sys7;
    systask command "&_s -termstmt %nrstr(%utl_slice(d:/wrx);) -log d:\log\a8.log -print d:\log\p8.txt" taskname=sys8;
    systask command "&_s -termstmt %nrstr(%utl_slice(e:/wrx);) -log d:\log\a9.log -print d:\log\p9.txt" taskname=sys9;
    systask command "&_s -termstmt %nrstr(%utl_slice(f:/wrx);) -log d:\log\a10.log -print d:\log\p10.txt" taskname=sys10;
    systask command "&_s -termstmt %nrstr(%utl_slice(g:/wrx);) -log d:\log\a11.log -print d:\log\p11.txt" taskname=sys11;
    systask command "&_s -termstmt %nrstr(%utl_slice(m:/wrx);) -log d:\log\a12.log -print d:\log\p12.txt" taskname=sys12;
    waitfor sys1 sys1 sys2 sys3 sys4 sys5 sys6 sys7 sys8 sys9 sys10 sys11 sys12 sys13 sys14 sys15 sys16;
    %put %sysevalf( %sysfunc(time()) - &tym);

    /*                                    _       _
    | | ___   __ _    ___  _ __   ___    (_) ___ | |__
    | |/ _ \ / _` |  / _ \| `_ \ / _ \   | |/ _ \| `_ \
    | | (_) | (_| | | (_) | | | |  __/   | | (_) | |_) |
    |_|\___/ \__, |  \___/|_| |_|\___|  _/ |\___/|_.__/
             |___/                     |__/
    */

    NOTE: Unable to open SASUSER.PROFILE. WORK.PROFILE will be opened instead.
    NOTE: All profile changes will be lost at the end of the session.
    NOTE: Copyright (c) 2016 by SAS Institute Inc., Cary, NC, USA.
    NOTE: SAS (r) Proprietary Software 9.4 (TS1M7)
          Licensed to THE CRITICAL PATH INSTITUTE, Site 70101661.
    NOTE: This session is executing on the X64_10PRO  platform.

     X64_10PRO WIN 10.0.19041  Workstation

    NOTE: SAS initialization used:
          real time           0.29 seconds
          cpu time            0.23 seconds


    NOTE: The file PY_PGM is:
          Filename=f:\wrk\_TD14284_T7610_\py_pgm.py,
          RECFM=V,LRECL=32766,File Size (bytes)=0,
          Last Modified=11Nov2021:12:46:09,
          Create Time=11Nov2021:12:46:09

    import pyreadstat
    fpath = 'c:/wrk/have.sas7bdat'
    df = pyreadstat.read_file_in_chunks(pyreadstat.read_sas7bdat, fpath, chunksize= 20000000)
    for df, meta in df:
        print(df)

    NOTE: 5 records were written to the file PY_PGM.
          The minimum record length was 384.
          The maximum record length was 384.
    NOTE: DATA statement used (Total process time):
          real time           0.01 seconds
          cpu time            0.01 seconds

    NOTE: The infile RUT is:
          Unnamed Pipe Access Device,

          PROCESS=c:\Python38\python.exe f:\wrk\_TD14284_T7610_\py_pgm.py 2>
          f:\wrk\_TD14284_T7610_/stderr.txt,
          RECFM=V,LRECL=32767

    NOTE: 78 lines were written to file PRINT.
    NOTE: 84 records were read from the infile RUT.
          The minimum record length was 0.
          The maximum record length was 43.
    NOTE: The DATA step printed pages 1-2.
    NOTE: DATA statement used (Total process time):
          real time           2:49.70
          cpu time            0.03 seconds


    NOTE: Fileref RUT has been deassigned.
    NOTE: Fileref PY_PGM has been deassigned.

    NOTE: SAS Institute Inc., SAS Campus Drive, Cary, NC USA 27513-2414
    NOTE: The SAS System used:
          real time           2:50.06
          cpu time            0.31 seconds

