// Could parse DICOM as well, encrypt most tag entries and send, on receive uncrypt
// anonymized processing pipeline

package main

import (
       "os"
       "io"
       "io/ioutil"
       "fmt"
       "os/user"
       "regexp"
       "encoding/json"
       "archive/zip"
       "bytes"
       "net/http"
       "path/filepath"
       "mime/multipart"
       "log"
       "github.com/codegangsta/cli"
)

const (
        GET_VIEW int = iota  
        GET_DOWNLOAD int = iota
        GET_LOG int = iota
)

func getMagickBoxes() {
  resp, err := http.Get("http://mmil.ucsd.edu/MagickBox/queryMachines.php")
  if err != nil {
    println("Error: could not query mmil.ucsd.edu")
  }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)

  // now parse the body
  var f interface{}
  json.Unmarshal(body, &f)
  m := f.([]interface{})
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
}

func getListOfJobs( reg string, ttype int) {

  machine, port, _ := getDefaultMagickBox()
  url := fmt.Sprintf("http://%v:%v/code/php/getScratch.php", machine, port)

  resp, err := http.Get(url)
  if err != nil {
    log.Fatal("Error: could not get list of jobs")
  }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)

  parseGet(body, reg, ttype)
}

func parseGet(buf []byte, reg string, ttype int) {
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

    if ttype == GET_VIEW || ttype == GET_LOG {
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

                if ttype == GET_LOG {
                   machine, port, _ := getDefaultMagickBox()
                   url := fmt.Sprintf("http://%v:%v/scratch/%v/processing.log", machine, port, scratchdir)

                   resp, err := http.Get(url)
                   if err != nil {
                      println("Error: could not get log for ", scratchdir)
                   }
                   defer resp.Body.Close()
                   body, err := ioutil.ReadAll(resp.Body)
                   bb["log"] = string(body)
                  
                   if search.MatchString(string(body)) {
                      foundOne = true
                   }
                }
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
          if ttype == GET_DOWNLOAD {
            downloadFile(scratchdir, pid)
          } else { // GET_VIEW and GET_LOG
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
    if ttype == GET_VIEW || ttype == GET_LOG {
      fmt.Printf("]\n")
    }
}

func downloadFile(scratchdir string, pid string) {

  machine, port, _ := getDefaultMagickBox()
  url := fmt.Sprintf("http://%v:%v/code/php/getOutputZip.php?folder=%s", machine, port, scratchdir)
  res, err := http.Get(url)
  if err != nil {
    log.Fatal(err)
  }
  defer res.Body.Close()

  filename := fmt.Sprintf("%s_%s.zip", pid, scratchdir)
  fp, _ := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0777)
  fmt.Println("writing", fp.Name())
  defer fp.Close() // defer close


  buf := make([]byte, 131072)
  var all []byte
  for {
    n, _ := res.Body.Read(buf)
    if n == 0 {
      break
    }
    all = append(all, buf[:n]...)
    fmt.Printf("receiving %3.2fmb\r", float64(len(all))/1024.0/1024.0)
  }
  _, err = fp.Write(all[:len(all)])
  if err != nil {
    log.Println(err)
  }

}

func newfileUploadRequest(uri string, params map[string]string, paramName, path string) (*http.Request, error) {
  file, err := os.Open(path)
  if err != nil {
      return nil, err
  }
  defer file.Close()

  body := &bytes.Buffer{}
  writer := multipart.NewWriter(body)
  part, err := writer.CreateFormFile(paramName, filepath.Base(path))
  if err != nil {
      return nil, err
  }
  _, err = io.Copy(part, file)

  for key, val := range params {
      _ = writer.WriteField(key, val)
  }
  err = writer.Close()
  if err != nil {
      return nil, err
  }

  request, err := http.NewRequest("POST", uri, body)
  request.Header.Add("Content-Type", writer.FormDataContentType())
  return request, err
}

func sendJob( aetitle string, dir string ) {
  machine, port, sender := getDefaultMagickBox()

  fmt.Printf("send directory \"%s\" for \"%s\" processing to %s:%s\n", dir, aetitle, machine, port)

  // walk through all the files in the directory
  buf := new(bytes.Buffer)

  w:= zip.NewWriter(buf)

  count := 0
  err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
     // we should check if we have a DICOM file... (or not)
     if err != nil {
      fmt.Println("Error:",err)
      return err
     }
     if info.IsDir() {
      return err
     }
     count = count + 1
     fmt.Printf("add file %v [%v]\r", path, count)
     // we could check if file is DICOM first... (seek 128bytes and see if we find "DICM")
     
     fname := fmt.Sprintf("file%05d_%v", count, filepath.Base(path))
     f, err := w.Create(fname)
     if err != nil {
      fmt.Printf("Error: could not create file in zip", err)
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
  workingdirectory := os.TempDir()

  fp, _ := ioutil.TempFile(workingdirectory, "mbsend_zip")
  //fmt.Println("store data for send in", fp.Name())
  zipFilename := fp.Name()
  _, err = buf.WriteTo(fp)
  //fmt.Printf("wrote bytes to zip file %v\n", n)
  if err != nil {
    fmt.Printf("error: ", err)
  }
  fp.Close()
  defer os.Remove(zipFilename)

  // 
  // Now send the new zip-file to the processing machine
  //
  extraParams := map[string]string{
      "aetitle": aetitle,
      "description": "Send by MagickBox",
      "filename": filepath.Base(zipFilename),
      "sender": sender,
  }
  url := fmt.Sprintf("http://%v:%v/code/php/processZip.php", machine, port)
  fmt.Println("Start sending compressed data...")
  request, err := newfileUploadRequest(url, extraParams, "theFile", zipFilename)
  if err != nil {
      log.Fatal(err)
  }
  client := &http.Client{}
  resp, err := client.Do(request)
  if err != nil {
      log.Fatal(err)
  } else {
/*      body := &bytes.Buffer{}
      _, err = body.ReadFrom(resp.Body)
      if err != nil {
          log.Fatal(err)
      }
      resp.Body.Close()
      if resp.StatusCode != 200 {
        fmt.Println(resp.StatusCode)
        fmt.Println(resp.Header)
        fmt.Println(body)
      }*/


  buf := make([]byte, 1024)
  var all []byte
  for {
    n, err := resp.Body.Read(buf)
    if n == 0 {
      break
    }
    if err != nil {
      log.Fatal(err)
    }
    all = append(all, buf[:n]...)
    fmt.Printf("writing %3.2fmb\r", float64(len(all))/1024.0/1024.0)
  }
  fp.Write(all)
  resp.Body.Close()



 /*   mr, err := request.MultipartReader()
    if err != nil {
        return
    }
    length := request.ContentLength
    for {

        part, err := mr.NextPart()
        if err == io.EOF {
            break
        }
        var read int64
        var p float32
        dst, err := os.OpenFile("dstfile", os.O_WRONLY|os.O_CREATE, 0644)
        if err != nil {
            return
        }
        for {
            buffer := make([]byte, 100000)
            cBytes, err := part.Read(buffer)
            if err == io.EOF {
                break
            }
            read = read + int64(cBytes)
            //fmt.Printf("read: %v \n",read )
            p = float32(read) / float32(length) *100
            fmt.Printf("progress: %v \n",p )
            dst.Write(buffer[0:cBytes])
        }
    }
*/


  }




/*  // send zip file for processing
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
  //easy.Setopt(curl.OPT_VERBOSE, true)

  easy.Setopt(curl.OPT_HTTPHEADER, []string{"Expect:"})

  //postdata := "aetitle=" + aetitle + "&filename=" + filepath.Base(zipFilename)
  //easy.Setopt(curl.OPT_POSTFIELDS, postdata)

  form := curl.NewForm()
  form.Add("aetitle", aetitle)
  form.Add("description", "Send by MagickBox")
  form.Add("filename", filepath.Base(zipFilename))
  // form.AddFile("theFile", "./readme.txt")
  if _, err := os.Stat(zipFilename); os.IsNotExist(err) {
    fmt.Printf("no such file or directory: %s", zipFilename)
    return
  }
  form.AddFile("theFile", zipFilename)

  easy.Setopt(curl.OPT_HTTPPOST, form)

  easy.Setopt(curl.OPT_NOPROGRESS, false)
  easy.Setopt(curl.OPT_PROGRESSFUNCTION, func(dltotal, dlnow, ultotal, ulnow float64, _ interface{}) bool {
    fmt.Printf("Uploading %3.2fmb\r", ulnow/1024/1024)
    return true
  })


  if err := easy.Perform(); err != nil {
    println("ERROR: ", err.Error())
  }
  println("")  // a last newline
  os.Remove(zipFilename)
  //time.Sleep(1000000000) // wait gorotine 

  */
}

func removeJobs( reg string ) {

  machine, port, _ := getDefaultMagickBox()
  url := fmt.Sprintf("http://%v:%v/code/php/getScratch.php", machine, port)

  resp, err := http.Get(url)
  if err != nil {
    println("Error: could not get list of jobs")
  }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)

  parseRemove(body, reg)
}


func parseRemove(buf []byte, reg string) {
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

    fmt.Printf("[")

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
            removeFile(scratchdir, pid)
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
    fmt.Printf("]\n")
}

func removeFile(scratchdir string, pid string) {
  machine, port, _ := getDefaultMagickBox()

  url := fmt.Sprintf("http://%v:%v/code/php/deleteStudy.php?scratchdir=%s", machine, port, scratchdir)

  resp, err := http.Get(url)
  if err != nil {
    println("Error: could not reach machine ", scratchdir)
  }

  defer resp.Body.Close()
}


func getDefaultMagickBox() (machine string, port string, sender string) {
            usr,_ := user.Current()
            dir := usr.HomeDir + "/.mb"
            type Machine struct {
              Machine string
              Port string
              Sender string
            }
            fi, err := os.Open(dir)
            if err != nil {
              // could be our first time, there is no .mb file, lets create a dummy file
              //log.Println("Error: configuration incomplete, call queryMachines/selectMachine/sender first...")
              saveDefaultMagickBox( "unknown", "unknown", "unknown" )
              fi, err = os.Open(dir)
              if err != nil {
                 log.Fatal("Error: Could not create default mb file at", dir)
              }
              return 
            }
            defer func() {
              if err := fi.Close(); err != nil {
                log.Fatal(err)
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
            sender = m.Sender
            return
}

func saveDefaultMagickBox( machine string, port string, sender string ) {
            usr,_ := user.Current()
            dir := usr.HomeDir + "/.mb"
            type Machine struct {
              Machine string
              Port string
              Sender string
            }
            m := Machine{Machine: machine, Port: port, Sender: sender}
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
            // println("set magick box to:", m.Machine, ":", m.Port, "(saved in", dir, ")")            
}

func saveSender( sender string ) {
  machine, port, _ := getDefaultMagickBox()
  saveDefaultMagickBox( machine, port, sender )
}

func getSender() (sender string) {
   _, _, sender = getDefaultMagickBox()
   return
}


func main() {
     app := cli.NewApp()
     app.Name = "mb"
     app.Usage = "MagickBox command shell for query, send, retrieve, and delete of data.\n\n" +
                 "   Start by listing known MagickBox instances (queryMachines). Identify your machine\n" +
                 "   and use selectMachine to specify it for all future commands. Also add your own\n" +
                 "   identity using the sender command. These steps need to be done only once.\n\n" +
                 "   Most calls return textual output in JSON format that can be processed by tools\n" +
                 "   such as jq (http://stedolan.github.io/jq/).\n\n" +
                 "   Regular expressions are used to identify individual sessions. They are applied\n" +
                 "   to all field values returned by the list command. If a session matches, the\n" +
                 "   command will be applied to it (list, push, pull, remove)."
     app.Version = "0.0.2"
     app.Author = "Hauke Bartsch"
     app.Email = "HaukeBartsch@gmail.com"
     app.Flags = []cli.Flag {
      cli.StringFlag {
        Name: "config-sender",
        Value: "",
        Usage: "Identify yourself, value is used as AETitleCaller [--config-sender <string>]",
      },
      cli.StringFlag {
        Name: "config-machine",
        Value: "",
        Usage: "Identify the IP address of the MagickBox you want to work with [--config-machine <string>]",
      },
      cli.StringFlag {
        Name: "config-port",
        Value: "",
        Usage: "Identify the port number used by your MagickBox [--config-port <string>]",
      },
     }

     // remember the default action (needs to be called if all the configurations fail to print help)
     defaultAction := app.Action
     app.Action = func(c *cli.Context) {
       if len(c.String("config-sender")) > 0  {
          if len(c.Args()) == 1 {
             saveSender( c.Args()[0] )
          } else {
             sender := getSender()
             fmt.Printf("{\"sender\": \"%s\"}\n", sender)
          }
       } else if len(c.String("config-machine")) > 0 {
          if len(c.Args()) == 1 {
             _, port, sender := getDefaultMagickBox()
             saveDefaultMagickBox( c.Args()[0], port, sender )
          } else {
             machine, _, _ := getDefaultMagickBox()
             fmt.Printf("{\"machine\": \"%s\"}\n", machine)
          }
       } else if len(c.String("config-port")) > 0 {
          if len(c.Args()) == 1 {
             machine, _, sender := getDefaultMagickBox()
             saveDefaultMagickBox( machine, c.Args()[0], sender )
          } else {
             _, port, _ := getDefaultMagickBox()
             fmt.Printf("{\"port\": \"%s\"}\n", port)
          }
       } else {
         defaultAction(c)
       }
     }

     app.Commands = []cli.Command{
     {
        Name:      "pull",
        ShortName: "g",
        Usage:     "Retrieve matching jobs [pull <regular expression>]",
        Description: "Download matching jobs as a zip file into the current directory.\n" +
                     "   Supply a regular expression to specify which session data to download.\n" +
                     "   Example:\n" +
                     "   > mb pull ip44\n" +
                     "   Downloads all sessions send from ip44.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) < 1 {
            fmt.Printf("Error: indiscriminate downloads are not supported, supply a regular expression that is matched against all fields\n")
          } else {
            getListOfJobs( c.Args().First(), GET_DOWNLOAD)
          }
        },
     },
     {

        Name:      "push",
        ShortName: "p",
        Usage:     "Send a directory for processing [push <aetitle> <dicom directory>]",
        Description: "Send a directory with DICOM data to the MagickBox for processing.\n" +
                     "   The <aetitle> is used to specify what type of processing will be run.\n\n" +
                     "   Example:\n" +
                     "   > mb push ProcFS53 /space/data/DICOMS/Subj001/\n" +
                     "   Sends the specified directory for FreeSurfer processing to the currently defined default MB instance.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) < 2 {
            fmt.Printf("Error: don't know what to send (usage: <aetitle> <directory to send>)\n")
          } else {
            sendJob( c.Args()[0], c.Args()[1] )
          }
        },
     },
     {
        Name:      "remove",
        ShortName: "r",
        Usage:     "Remove data [remove <regular expression>]",
        Description: "Remove session data stored in MagickBox.\n\n" +
                     "   Example:\n" +
                     "   > mb remove tmp.1234567\n" +
                     "   Removes a specific study identified by its scratchdir.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) < 1 {
            fmt.Printf("Error: It is not allowed to remove indiscriminately sessions from MagickBox, provide a regular expression.\n")
          } else {
            removeJobs( c.Args()[0] )
          }
        },
     },
     {
        Name:      "list",
        ShortName: "l",
        Usage:     "Show list of matching jobs [list [regular expression]]",
        Description: "Display a list of matching jobs.\n\n" +
                     "   Example:\n" +
                     "   > mb list\n" +
                     "   Returns a list of all jobs as JSON.\n\n" +
                     "   Example:\n" +
                     "   > mb list ip44\n" +
                     "   Returns a list of all jobs that have been send from ip44.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) == 1 {
            getListOfJobs( c.Args()[0], GET_VIEW )
          } else {
            getListOfJobs( ".*", GET_VIEW )
          }
        },
     },
     {
        Name:      "log",
        ShortName: "l",
        Usage:     "Show processing log of matching jobs [log [regular expression]]",
        Description: "Display a list of matching jobs with log entries. This is very similar to the list\n" +
                     "   command but requires a separate call to MagickBox for each job (might be slow).\n\n" +
                     "   Example:\n" +
                     "   > mb log\n" +
                     "   Returns all processing logs in json format.\n\n" +
                     "   Example:\n" +
                     "   > mb log ip44\n" +
                     "   Returns a list of all processing logs that have been send from ip44.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) == 1 {
            getListOfJobs( c.Args()[0], GET_LOG )
          } else {
            getListOfJobs( ".*", GET_LOG )
          }
        },
     },
     {
        Name:      "queryMachines",
        ShortName: "q",
        Usage:      "Display list of known MagickBox instances [queryMachines]",
        Description: "Display a list of all known MagickBox machines. This feature uses\n" +
                     "   a centralized service hosted at the MMIL.\n",
        Action: func(c *cli.Context) {
            getMagickBoxes()
        },
     },
     {
        Name:      "setMachine",
        ShortName: "s",
        Usage:      "Specify the default MagickBox [setMachine [<IP> <port>]]",
        Description: "Without any arguments this call will return the default MagickBox.\n" +
                     "   Specify the internet address (IP) and the port number to change the default\n" +
                     "   MagickBox used for subsequent calls of this tool.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) == 2 {
             _, _, sender := getDefaultMagickBox()
             saveDefaultMagickBox( c.Args()[0], c.Args()[1], sender )
          } else {
            machine, port, _ := getDefaultMagickBox()
            fmt.Printf("{\"machine\": \"%s\", \"port\": \"%s\"}\n", machine, port)
          }
        },
     },
     {
        Name:      "setSender",
        ShortName: "w",
        Usage:      "Specify a string identifying the sender [setSender [<sender>]]",
        Description: "Without any arguments this call will return the current sender.\n" +
                     "   Specify the sender string used as AETitle of the caller for subsequent calls of this tool.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) == 1 {
             saveSender( c.Args()[0] )
          } else {
            sender := getSender()
            fmt.Printf("{\"sender\": \"%s\"}\n", sender)
          }
        },
     },
  }


  app.Run(os.Args)
}