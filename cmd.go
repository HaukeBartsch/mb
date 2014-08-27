package main

import (
       "os"
       "io/ioutil"
       "fmt"
       "os/user"
       "strconv"
       "regexp"
       "encoding/json"
       "archive/zip"
       "bytes"
       "time"
       "path/filepath"
       "github.com/codegangsta/cli"
       "github.com/andelf/go-curl"
)

// get json list of magick box machines known to the mmil.ucsd.edu server
func getMagickBoxes() {
  easy := curl.EasyInit()
  defer easy.Cleanup()

  easy.Setopt(curl.OPT_URL, "http://mmil.ucsd.edu/MagickBox/queryMachines.php")
  easy.Setopt(curl.OPT_PORT, 80)

  // make a callback function
  getTest := func( buf []byte, userdata interface{}) bool {
    //println("DEBUG: size=>", len(buf))
    //println("DEBUG: content=>", string(buf))

    var f interface{}
    json.Unmarshal(buf, &f)
    m := f.([]interface{})
    /* if (len(m) > 1) {
      println("found", len(m), "machines")
    } else {
      println("found", len(m), "machine")
    } */
    fmt.Printf("[")
    for k2, v2 := range m {
        bb := v2.(map[string]interface{})
        machine := ""
        port := ""
        for k, v := range bb {
          switch vv := v.(type) {
          case int:
              //fmt.Printf(k, "is an int", vv)
          case string:
              // fmt.Printf("%d %v: %v\n", k2, k, vv)
            if k == "machine" {
              machine = vv
            }
            if k == "port" {
                port = vv
            }
          case []interface{}:
              //for i, u := range vv {
              //   fmt.Println(k2, " ", k, " ", i, "is string", u)
              //}
          default:
              //fmt.Println(k, "is of a type I don't know how to handle")
          }
        }
        fmt.Printf("{ \"id\": \"%d\", \"machine\": \"%v\", \"port\": \"%v\" }", k2, machine, port)
        if k2 < len(m)-1 {
            fmt.Printf(",")
        }
    }
    fmt.Printf("]\n")

    return true
  }

  easy.Setopt(curl.OPT_WRITEFUNCTION, getTest)

  if err := easy.Perform(); err != nil {
    fmt.Printf("ERROR: %v\n", err)
  }
}

// print out json of job list from server (argument limits the list by regexp on values for each job)
func getListOfJobs( reg string, download bool ) {
  easy := curl.EasyInit()
  defer easy.Cleanup()

  machine, port := getDefaultMagickBox()
  var url string
  var portNumber int
  portNumber, err := strconv.Atoi(port)
  if err != nil {
    fmt.Printf("Error: could not convert port number to int %v", err);
  }
  url = fmt.Sprintf("http://%v/code/php/getScratch.php", machine)
  //fmt.Printf("url is : %v ", url)
  easy.Setopt(curl.OPT_URL, url)
  easy.Setopt(curl.OPT_PORT, portNumber)

  // make a callback function
  easy.Setopt(curl.OPT_WRITEFUNCTION, func( buf []byte, userdata interface{} ) bool {
    file := userdata.(*os.File)
    if _, err := file.Write(buf); err != nil {
      return false
    }
    return true
  })
  fp, _ := ioutil.TempFile("", "mbretrieve") // os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
  //fmt.Println("write temp file to ", fp.Name())
  filename := fp.Name()
  // defer fp.Close() // defer close

  easy.Setopt(curl.OPT_WRITEDATA, fp)

  if err := easy.Perform(); err != nil {
    fmt.Printf("ERROR: %v\n", err)
  }

  // function should be called after all the entries have been written to disk
  defer func(filename string, fp *os.File, reg string) {
    // read the file in again and parse it
    fp.Close()
    data, err := ioutil.ReadFile(filename)
    if err != nil {
      fmt.Println("Error: could not read temporary file again ", filename)
      return
    }
    parseGet(data, reg, download)
  }(filename, fp, reg)

}

func parseGet(buf []byte, reg string, download bool) {
    var search = regexp.MustCompile(reg)
    var scratchDirReg = regexp.MustCompile("scratchdir")
    var pidReg = regexp.MustCompile("pid")
    var f interface{}
    err := json.Unmarshal(buf[:len(buf)], &f)
    if err != nil {
      fmt.Printf("%v\n%s\n\n", err, buf)
    }

    //var fil string
    //fil = userdata.(string)
    //fmt.Printf("info: search for %v\n", fil)

    if download == false {
      fmt.Printf("[")
    }

    // get array of structs and test each value for the reg string
    m := f.([]interface{})
    count := 0
    for _, v2 := range m {
        bb := v2.(map[string]interface{})
        var scratchdir = ""
        var pid = ""
        var foundOne = false
        for k, v := range bb {
          switch vv := v.(type) {
          case string:
              //fmt.Printf("%d %v: %v\n", k2, k, vv)
              //fmt.Println(search.MatchString(vv))
              if scratchDirReg.MatchString(k) {
                scratchdir = vv
              }
              if pidReg.MatchString(k) {
                pid = vv
              }

              if search.MatchString(vv) {
                foundOne = true
              }
          default:
              //fmt.Println(k, "is of a type I don't know how to handle")
          }
        }
        if foundOne {
          if download {
            downloadFile(scratchdir, pid)
          } else {
            if count > 0 {
              fmt.Printf(",")
            }
            count = count + 1
            b, err := json.MarshalIndent(bb, "", "  ")
            if err != nil {
              fmt.Println("error:", err)
            }
            os.Stdout.Write(b)
          }
        }
    }
    if download == false {
      fmt.Printf("]\n")
    }
}

func downloadFile(scratchdir string, pid string) {
  easy := curl.EasyInit()
  defer easy.Cleanup()

  machine, port := getDefaultMagickBox()
  //fmt.Printf("using: %v on %v\n", machine, port)
  var url string
  var portNumber int
  portNumber, err := strconv.Atoi(port)
  if err != nil {
    fmt.Printf("Error: could not convert port number to int %v", err);
  }
  url = fmt.Sprintf("http://%v/code/php/getOutputZip.php?folder=%s", machine, scratchdir)

  easy.Setopt(curl.OPT_URL, url)
  easy.Setopt(curl.OPT_PORT, portNumber)

  // make a callback function
  easy.Setopt(curl.OPT_WRITEFUNCTION, func( buf []byte, userdata interface{} ) bool {

    file := userdata.(*os.File)
    if _, err := file.Write(buf); err != nil {
      return false
    }
    return true
  })

  // create a temporary file
  filename := fmt.Sprintf("%s_%s.zip", pid, scratchdir)
  fp, _ := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
  fmt.Println("writing", fp.Name())
  defer fp.Close() // defer close

  easy.Setopt(curl.OPT_WRITEDATA, fp)
  easy.Setopt(curl.OPT_NOPROGRESS, false)
  easy.Setopt(curl.OPT_PROGRESSFUNCTION, func(dltotal, dlnow, ultotal, ulnow float64, _ interface{}) bool {
    fmt.Printf("Download %3.2fmb, Uploading %3.2f\r", dlnow/1024/1024, ulnow/1024/1024)
    return true
  })

  if err := easy.Perform(); err != nil {
    fmt.Printf("ERROR: %v\n", err)
  }
}

func sendJob( aetitle string, dir string ) {
  fmt.Printf("send a directory \"%s\" for processing to \"%s\"\n", dir, aetitle)

  // walk through all the files in the directory
  buf := new(bytes.Buffer)

  w:= zip.NewWriter(buf)

  err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
     // we should check if we have a DICOM file... (or not)
     if err != nil {
      fmt.Println("Error:",err)
      return err
     }
     if info.IsDir() {
      return err
     }
     fmt.Printf("add file %v\r", path)
     // we could check if file is DICOM first... (seek 128bytes and see if we find "DICM")

     f, err := w.Create(path)
     if err != nil {
      fmt.Printf("Error: could not create path in zip", err)
     }
     data, readerr := ioutil.ReadFile(path)
     if readerr != nil {
      fmt.Printf("Error: could not read content of", path)
     }

     _, err = f.Write([]byte(data))
     if err != nil {
       fmt.Printf("Error: could not write file to zip")
     }

     return err
  })
  if err != nil {
    fmt.Println("Error: could not read directory")
    return
  }

  err = w.Close()
  if err != nil {
    fmt.Println("error:", err)
  }
  fmt.Println("")

  // write buffer to file
  workingdirectory, err := os.Getwd()
  if err != nil {
    fmt.Println("Error: could not get current working directory")
  }
  fp, _ := ioutil.TempFile(workingdirectory, "mbsend_zip")
  fmt.Println("store directory temporarily in ", fp.Name())
  zipFilename := fp.Name()
  n, err := buf.WriteTo(fp)
  fmt.Printf("wrote bytes to zip file %v\n", n)
  if err != nil {
    fmt.Printf("error: ", err)
  }
  fp.Close()

  machine, port := getDefaultMagickBox()

  // send zip file for processing
  easy := curl.EasyInit()
  defer easy.Cleanup()

  url := fmt.Sprintf("http://%v/code/php/processZip.php", machine)
  var portNumber int
  portNumber, err = strconv.Atoi(port)
  if err != nil {
    fmt.Printf("Error: could not convert port number to int %v", err);
    return
  }
  easy.Setopt(curl.OPT_URL, url)
  easy.Setopt(curl.OPT_PORT, portNumber)
  easy.Setopt(curl.OPT_POST, true)
  easy.Setopt(curl.OPT_VERBOSE, true)

  easy.Setopt(curl.OPT_HTTPHEADER, []string{"Expect:"})

  //postdata := "aetitle=" + aetitle + "&filename=" + filepath.Base(zipFilename)
  //easy.Setopt(curl.OPT_POSTFIELDS, postdata)

  form := curl.NewForm()
  form.Add("aetitle", aetitle)
  form.Add("description", "Send by MB")
  form.Add("filename", filepath.Base(zipFilename))
  // form.AddFile("theFile", "./readme.txt")
  if _, err := os.Stat(zipFilename); os.IsNotExist(err) {
    fmt.Printf("no such file or directory: %s", zipFilename)
    return
  }
  form.AddFile("theFile", zipFilename)
  fmt.Println("send the file:", zipFilename)

  easy.Setopt(curl.OPT_HTTPPOST, form)

  easy.Setopt(curl.OPT_NOPROGRESS, false)
  easy.Setopt(curl.OPT_PROGRESSFUNCTION, func(dltotal, dlnow, ultotal, ulnow float64, _ interface{}) bool {
    fmt.Printf("Download %3.2fmb, Uploading %3.2f\r", dlnow/1024/1024, ulnow/1024/1024)
    return true
  })


  if err := easy.Perform(); err != nil {
    println("ERROR: ", err.Error())
  }

  time.Sleep(1000000000) // wait gorotine
}

func getDefaultMagickBox() (machine string, port string) {
            usr,_ := user.Current()
            dir := usr.HomeDir + "/.mb"
            type Machine struct {
              Machine string
              Port string
            }
            fi, err := os.Open(dir)
            if err != nil { panic(err) }
            defer func() {
              if err := fi.Close(); err != nil {
                panic(err)
              }
            }()
            buf := make([]byte, 1024)
            n, err := fi.Read(buf)
            if err != nil {
              panic(err)
            }
            // os.Stdout.Write(buf)
            var m Machine
            err = json.Unmarshal(buf[:n], &m)
            if err != nil {
              fmt.Println("Error: no default machine setup ->", err)
            }
            machine = m.Machine
            port = m.Port
            return
}

func saveDefaultMagickBox( machine string, port string ) {
            usr,_ := user.Current()
            dir := usr.HomeDir + "/.mb"
            type Machine struct {
              Machine string
              Port string
            }
            m := Machine{Machine: machine, Port: port}
            b, err := json.Marshal(m)
            // os.Stdout.Write(b)
            if err != nil { panic(err) }

            fi, err := os.Create(dir)
            if err != nil { panic(err) }
            defer func() {
              if err := fi.Close(); err != nil {
                panic(err)
              }
            }()
            n := len(b)
            if _, err := fi.Write(b[:n]); err != nil {
              panic(err)
            }
            println("set magick box to:", m.Machine, ":", m.Port, "(saved in", dir, ")")            
}

func main() {
     app := cli.NewApp()
     app.Name = "mb"
     app.Usage = "command shell for query, send, and retrieve"
     app.Commands = []cli.Command{
     {
        Name:      "get",
        ShortName: "g",
        Usage:     "retrieve matching jobs as a zip file (supply a regular expression)",
        Action: func(c *cli.Context) {
          if len(c.Args()) < 1 {
            fmt.Printf("Error: don't know what to download, supply a reglar expression that is matched against all fields\n")
          } else {
            getListOfJobs( c.Args().First(), true)
          }
        },
     },
     {
        Name:      "push",
        ShortName: "p",
        Usage:     "send a directory as zip file for processing (<aetitle> <dicom directory>)",
        Action: func(c *cli.Context) {
          if len(c.Args()) < 2 {
            fmt.Printf("Error: don't know what to send (usage: <aetitle> <directory to send>)\n")
          } else {
            sendJob( c.Args()[0], c.Args()[1] )
          }
        },
     },
     {
        Name:      "list",
        ShortName: "l",
        Usage:     "get list of matching jobs (supply a regular expression)",
        Action: func(c *cli.Context) {
          if len(c.Args()) == 1 {
            getListOfJobs( c.Args()[0], false )
          } else {
            getListOfJobs( ".*", false )
          }
        },
     },
     {
        Name:      "queryMachines",
        ShortName: "q",
        Usage:      "query the list of MagickBox machines (json format)",
        Action: func(c *cli.Context) {
            //println("query task: ", c.Args().First())
            getMagickBoxes()
        },
     },
     {
        Name:      "selectMachine",
        ShortName: "s",
        Usage:      "set a magick box default machine (<IP> <port>), without arguments print out current default",
        Action: func(c *cli.Context) {
          if len(c.Args()) == 2 {
             saveDefaultMagickBox( c.Args()[0], c.Args()[1])
          } else {
            machine, port := getDefaultMagickBox()
            fmt.Printf("{\"machine\": \"%s\", \"port\": \"%s\"}\n", machine, port)
            // println("Error: call selectMachine with <ip> <port>")
          }
        },
     },
  }


  app.Run(os.Args)
}