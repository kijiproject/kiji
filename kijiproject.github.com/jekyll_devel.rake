
require 'rubygems'
require 'rake'
require 'fileutils'
require 'pathname'

##
#  jekyll_devel.rake addresses the growing amount of time that Jekyll
#  takes to generate HTML from markdown.  This time seriously affects the 
#  Edit -> Jekyll generate -> preview -> validate loop.
#
#  After scouring the internet it seems this is a popular request for
#  the Jekyll development, and the few people that have managed to find 
#  workarounds that are fairly "hackish".  Here's the WibiData version
#  of that hack.
#
#  On investigation, it appears that jekyll's runtime is almost completely
#  consumed in processing items in the _posts directory. 
#
#  It's my understanding that the most common workflow is to just work
#  on the files in "DEVEL" directories.  This rakefile will only generate
#  a preview of these directories.
#  
#  Suppose you only want to edit the DEVEL docs  You'd run this command:
#
#    rake -f jekyll_devel.rake jekyll_devel:preview
#
#  On my macbook pro I can generate the DEVEL docs in about 3 seconds.
#
#  Additionally I added the -w flag to the jekyll preview so that pages
#  can be regenerated on the fly, while preview is running.
#
#  I saw one additional tool called liveReload ($9.99 at the app store).
#  It has the ability to watch the filesystem and will reload the browser.
##

namespace :jekyll_devel do

  desc 'Launch preview environmentfor Devel'
  task :preview
    puts 'executing preview env for posts named \"DEVEL\"'


    trap 'INT', 'IGNORE'
    
    Dir.glob './_posts/**/*/'  do | src |
      dest = src.gsub /_posts/, '_shelf'
      pn = Pathname.new dest
      dest=pn.dirname
      if /[0-9]*\.[0-9]*\.[0-9]*\/$/ =~ src
        FileUtils.mkdir_p dest, :verbose=>true 
        FileUtils.mv src, dest, :verbose=>true
      end
    end
    
    system "./scripts/run-server.sh"

    Dir.foreach './_shelf'  do | entry |
      if entry != '.' && entry != '..'
        FileUtils.cp_r "./_shelf/#{entry}", './_posts/', :verbose=>true
        FileUtils.remove_dir("./_shelf/#{entry}", :verbose=>true )
      end
    end
    trap('INT', 'DEFAULT')
  end # task :preview
