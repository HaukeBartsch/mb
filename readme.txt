readme.txt

It would be nice to have an interface that can connect to existing MagickBox machines. The scripting interface would allow to list MagickBox machines (like databases). And query their content (list of tables). The interface would basically use curl internally to send commands and provide the output. Operations that should be supported are:

 - MagickBox discovery
 - bucket discovery
 - bucket transfer
 - data push/pull to buckets



Build:
   go install
   (in source directory)
   go build
   (in current directory)

Run:
   ../../../../bin/mb q