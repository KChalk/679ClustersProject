base_url = "http://files.pushshift.io/reddit/comments/"

comment_filenames = ["RC_2007-10.bz2",
"RC_2007-11.bz2",
"RC_2007-12.bz2",
"RC_2008-01.bz2",
"RC_2008-02.bz2",
"RC_2008-03.bz2",
"RC_2008-04.bz2",
"RC_2008-05.bz2",
"RC_2008-06.bz2",
"RC_2008-07.bz2",
"RC_2008-08.bz2",
"RC_2008-09.bz2",
"RC_2008-10.bz2",
"RC_2008-11.bz2",
"RC_2008-12.bz2",
"RC_2009-01.bz2",
"RC_2009-02.bz2",
"RC_2009-03.bz2",
"RC_2009-04.bz2",
"RC_2009-05.bz2",
"RC_2009-06.bz2",
"RC_2009-07.bz2",
"RC_2009-08.bz2",
"RC_2009-09.bz2",
"RC_2009-10.bz2",
"RC_2009-11.bz2",
"RC_2009-12.bz2",
"RC_2010-01.bz2",
"RC_2010-02.bz2",
"RC_2010-03.bz2",
"RC_2010-04.bz2",
"RC_2010-05.bz2",
"RC_2010-06.bz2",
"RC_2010-07.bz2",
"RC_2010-08.bz2",
"RC_2010-09.bz2",
"RC_2010-10.bz2",
"RC_2010-11.bz2",
"RC_2010-12.bz2",
"RC_2011-01.bz2",
"RC_2011-02.bz2",
"RC_2011-03.bz2",
"RC_2011-04.bz2",
"RC_2011-05.bz2",
"RC_2011-06.bz2",
"RC_2011-07.bz2",
"RC_2011-08.bz2",
"RC_2011-09.bz2",
"RC_2011-10.bz2",
"RC_2011-11.bz2",
"RC_2011-12.bz2",
"RC_2012-01.bz2",
"RC_2012-02.bz2",
"RC_2012-03.bz2",
"RC_2012-04.bz2",
"RC_2012-05.bz2",
"RC_2012-06.bz2",
"RC_2012-07.bz2",
"RC_2012-08.bz2",
"RC_2012-09.bz2",
"RC_2012-10.bz2",
"RC_2012-11.bz2",
"RC_2012-12.bz2",
"RC_2013-01.bz2",
"RC_2013-02.bz2",
"RC_2013-03.bz2",
"RC_2013-04.bz2",
"RC_2013-05.bz2",
"RC_2013-06.bz2",
"RC_2013-07.bz2",
"RC_2013-08.bz2",
"RC_2013-09.bz2",
"RC_2013-10.bz2",
"RC_2013-11.bz2",
"RC_2013-12.bz2",
"RC_2014-01.bz2",
"RC_2014-02.bz2",
"RC_2014-03.bz2",
"RC_2014-04.bz2",
"RC_2014-05.bz2",
"RC_2014-06.bz2",
"RC_2014-07.bz2",
"RC_2014-08.bz2",
"RC_2014-09.bz2",
"RC_2014-10.bz2",
"RC_2014-11.bz2",
"RC_2014-12.bz2",
"RC_2015-01.bz2",
"RC_2015-02.bz2",
"RC_2015-03.bz2",
"RC_2015-04.bz2",
"RC_2015-05.bz2",
"RC_2015-06.bz2",
"RC_2015-07.bz2",
"RC_2015-08.bz2",
"RC_2015-09.bz2",
"RC_2015-10.bz2",
"RC_2015-11.bz2",
"RC_2015-12.bz2",
"RC_2016-01.bz2",
"RC_2016-02.bz2",
"RC_2016-03.bz2",
"RC_2016-04.bz2",
"RC_2016-05.bz2",
"RC_2016-06.bz2",
"RC_2016-07.bz2",
"RC_2016-08.bz2",
"RC_2016-09.bz2",
"RC_2016-10.bz2",
"RC_2016-11.bz2",
"RC_2016-12.bz2",
"RC_2017-01.bz2",
"RC_2017-02.bz2",
"RC_2017-03.bz2",
"RC_2017-04.bz2",
"RC_2017-05.bz2",
"RC_2017-06.bz2",
"RC_2017-07.bz2",
"RC_2017-08.bz2",
"RC_2017-09.bz2",
"RC_2017-10.bz2",
"RC_2017-11.bz2",
"RC_2017-12.xz",
"RC_2018-01.xz",
"RC_2018-02.xz",
"RC_2018-03.xz",
"RC_2018-04.xz",
"RC_2018-05.xz",
"RC_2018-06.xz",
"RC_2018-07.xz",
"RC_2018-08.xz",
"RC_2018-09.xz"
]

comment_filenames.each do |c|
  full_url = "#{base_url}/#{c}"
  puts full_url
  if File.exists? c
    puts "#{c} exists, skipping..."
  else
    `wget #{full_url} -q`
  end
end
