// TODO:

// Could parse DICOM as well, encrypt most tag entries and send, on receive uncrypt
// anonymized processing pipeline

// Add interface to configure processing pipelines, this would include aseg or surface,
// do gradunwarp or not etc.
// Started implementing by supportign arguments to push ('arguments' string that is 
// send over to processZip.php)

// Make mb read and write json from stdin (first list, second pull on that list).

// Make current magickbox machine a list and do round-robin and distributed list/log.

package main

import (
       "os"
       "io"
       "io/ioutil"
       "fmt"
       "os/user"
       "time"
       "regexp"
       "encoding/json"
       "math/rand"
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

type Machine struct {
  Machine string
  Port string
  Sender string
}

// structure returned by getInstalledBuckets + machine and port
type Process struct{
  AETitle string
  Description string
  License string
  Name string
  Version string
  Machine string
  Port string
}

type Settings struct {
  Sender string
}


func printListOfMagickBoxes( f interface{} ) {

  b, err := json.MarshalIndent(f, "", "  ")
  if err != nil {
    fmt.Println("error:", err)
  }
  fmt.Printf("%v\n", string(b))

/*  m := f.([]interface{})
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
  fmt.Printf("]\n") */

}


// return all MagickBox machines known
// todo: check if they are visible as well 
func getMagickBoxes() (interface{}) {
  resp, err := http.Get("http://mmil.ucsd.edu/MagickBox/queryMachines.php")
  if err != nil {
    println("Error: could not query mmil.ucsd.edu")
  }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)

  // now parse the body
  var f interface{}
  json.Unmarshal(body, &f) // unnamed structure we will print everything that comes back

  return f
}

func getListOfJobs( reg string, ttype int) {

  var all_ms []Machine = getActiveMagickBoxes()
  var results []map[string]interface{}
  c := make(chan []map[string]interface{})

  for _,v := range all_ms {

    go func(machine string, port string) {

      url := fmt.Sprintf("http://%v:%v/code/php/getScratch.php", machine, port)

      resp, err := http.Get(url)
      if err != nil {
        log.Fatal("Error: could not get list of jobs")
      }
      defer resp.Body.Close()
      body, err := ioutil.ReadAll(resp.Body)

      var res = parseGet(body, reg, ttype, machine, port)

      for _,v := range res {
        v["Machine"] = machine
        v["Port"] = port
      }
      //fmt.Println("put something into the channel")
      c <- res
    }(v.Machine, v.Port)
  }
  timeout := time.After(5000 * time.Millisecond)
  loop:
  for i := 0; i < len(all_ms); i++ {
    select {
    case res := <-c:
      results = append(results, res...)
      //fmt.Println("got something from one go routing...")
      //break
    case <-timeout:
      fmt.Fprintf(os.Stderr, "Warning: %v machines did not answer in time...", len(all_ms)-i)
      break loop
    }
  }

  if ttype == GET_VIEW || ttype == GET_LOG {
    b, err := json.MarshalIndent(results, "", "  ")
    if err != nil {
      fmt.Println("Error: could not print result")
    }
    os.Stdout.Write(b)
  }
}

func parseGet(buf []byte, reg string, ttype int, machine string, port string) ( []map[string]interface{} ) {
    var search = regexp.MustCompile(reg)
    var scratchDirReg = regexp.MustCompile("scratchdir")
    var pidReg = regexp.MustCompile("pid")
    var returned []map[string]interface{}

    var f interface{}
    err := json.Unmarshal(buf[:len(buf)], &f)
    if err != nil {
      fmt.Printf("%v\n%s\n\n", err, buf)
    }

    // get array of structs and test each value for the reg string
    m := f.([]interface{})
    //count := 0
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
                   //machine, port, _ := getDefaultMagickBox()
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
            downloadFile(scratchdir, pid, machine, port)
          } else { // GET_VIEW and GET_LOG
            returned = append(returned, bb)
          }
        }
    }
    return returned
}

func downloadFile(scratchdir string, pid string, machine string, port string) {

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

func sendJob( aetitle string, dir string, arguments string) {

  var all_ms []Machine = getActiveMagickBoxes()
  // which machine should we use?
  // a machine which has the processing (AETitle) that is requested
  // from all the remaining machines pick one randomly (should check for load)
  var ms_valid []Machine
  for _,v := range all_ms {
    // get the list of processing options
    var procs []Process = getInstalledBuckets(v.Machine, v.Port, nil)
    for _,vv := range procs {
      if vv.AETitle == aetitle {
         ms_valid = append(ms_valid, v)
         break
      }
    }
  }
  if len(ms_valid) == 0 {
    fmt.Println("Error: no machine found that provides aetitle %v", aetitle)
    return
  }
  // for now just pick randomly (should query which machine has lower load)
  var pick = rand.Intn(len(ms_valid))
  machine := ms_valid[pick].Machine
  port    := ms_valid[pick].Port
  sender  := getSender()

  fmt.Printf("send directory \"%s\" as \"%v\" for \"%s\" processing to %s:%s (%v machines provide this feature)\n", 
      dir, sender, aetitle, machine, port, len(ms_valid))

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
      "arguments": arguments,
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
    // this does not work
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

  }

}

// jobs will be removed from all active machines
func removeJobs( reg string ) {

  // all active machines
  var ms []Machine = getActiveMagickBoxes()
  // return values from each machine
  var removed []map[string]interface{}
  // collect the results
  c := make(chan []map[string]interface{})
  for _, v := range ms {

    go func(machine string, port string) {
      url := fmt.Sprintf("http://%v:%v/code/php/getScratch.php", machine, port)

      resp, err := http.Get(url)
      if err != nil {
        println("Error: could not get list of jobs")
      }
      defer resp.Body.Close()
      body, err := ioutil.ReadAll(resp.Body)

      var rem = parseRemove(body, reg, machine, port)
      for _,v := range rem {
        v["Machine"] = machine
        v["Port"] = port
      }
      c <- rem
    }(v.Machine, v.Port)
  }
  timeout := time.After(500 * time.Millisecond)
  loop:
  for i := 0; i < len(ms); i++ {
    select {
      case  rem := <-c:
        removed = append(removed, rem...)
        break
      case <-timeout:
        fmt.Fprintf(os.Stderr, "Warning: we timed out, some machines might have taken too long to answer")
        break loop
    }
  }

  b, err := json.MarshalIndent(removed, "", "  ")
  if err != nil {
    fmt.Println("error:", err)
  }
  os.Stdout.Write(b)
}

// returns the entries that have been removed
func parseRemove(buf []byte, reg string, machine string, port string) ( removed []map[string]interface{} ) {
    var search = regexp.MustCompile(reg)
    var scratchDirReg = regexp.MustCompile("scratchdir")
    var pidReg = regexp.MustCompile("pid")
    var returned []map[string]interface{}

    var f interface{}
    err := json.Unmarshal(buf[:len(buf)], &f)
    if err != nil {
      fmt.Printf("%v\n%s\n\n", err, buf)
    }

    //fmt.Printf("[")

    // get array of structs and test each value for the reg string
    m := f.([]interface{})
    //count := 0
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
            removeFile(scratchdir, pid, machine, port)
            returned = append(returned, bb)
            /*if count > 0 {
              fmt.Printf(",")
            }
            count = count + 1
            b, err := json.MarshalIndent(bb, "", "  ")
            if err != nil {
              fmt.Println("error:", err)
            }
            os.Stdout.Write(b) */
        }
    }
    //fmt.Printf("]\n")
    return returned
}

func removeFile(scratchdir string, pid string, machine string, port string) {

  url := fmt.Sprintf("http://%v:%v/code/php/deleteStudy.php?scratchdir=%s", machine, port, scratchdir)

  timeout := time.Duration(5 * time.Second)
  client := http.Client{ Timeout: timeout }
  resp, err := client.Get(url)
  if err != nil {
    println("Error: could not reach machine ", scratchdir)
  }

  defer resp.Body.Close()
}

func saveActiveMagickBoxes( ms []Machine ) {
  usr,_ := user.Current()
  dir := usr.HomeDir + "/.magickbox"
  b, err := json.MarshalIndent(ms, "", "  ")
  if err != nil { 
    panic(err) 
  }

  fi, err := os.Create(dir)
  if err != nil { 
    panic(err) 
  }
  defer func() {
    if err := fi.Close(); err != nil {
      panic(err)
    }
  }()
  n := len(b)
  if _, err := fi.Write(b[:n]); err != nil {
    panic(err)
  }
}

func getActiveMagickBoxes() ( []Machine ) {

  var ms = []Machine{}

  usr,_ := user.Current()
  dir := usr.HomeDir + "/.magickbox"
  fi, err := os.Open(dir)
  if err != nil {
    // could be our first time, there is no .mb file, lets create a dummy file
    //log.Println("Error: configuration incomplete, call queryMachines/selectMachine/sender first...")
    // saveDefaultMagickBox( "unknown", "unknown", "unknown" )
    var machines []Machine = []Machine{}
    saveActiveMagickBoxes( machines )

    fi, err = os.Open(dir)
    if err != nil {
      log.Fatal("Error: Could not create default mb file at", dir)
    }
    return ms
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
  err = json.Unmarshal(buf[:n], &ms)
  if err != nil {
    fmt.Println("Error: no default machine setup ->", err)
  }
  if len(ms) == 0 {
    fmt.Println("Warning: no active machines defined, use 'mb queryMachines' followed by 'mb activeMachines add <ip> <port>' to create some")
  }

  return ms
}


func saveSender( sender string ) {
  usr,_ := user.Current()
  dir := usr.HomeDir + "/.mb"
  m := Settings{Sender: sender}

  b, err := json.MarshalIndent(m, "", "  ")
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
}

func getSender() (sender string) {
  usr,_ := user.Current()
  dir := usr.HomeDir + "/.mb"
  fi, err := os.Open(dir)
  if err != nil {
    // could be our first time, there is no .mb file, lets create a dummy file
    //log.Println("Error: configuration incomplete, call queryMachines/selectMachine/sender first...")
    saveSender("unknown")
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
  var m Settings
  err = json.Unmarshal(buf[:n], &m)
  if err != nil {
    fmt.Println("Error: no default machine setup ->", err)
  }
  sender = m.Sender
  return
}

func getInstalledBuckets(machine string, port string, filter *regexp.Regexp) ([]Process) {

  var results []Process
  url := fmt.Sprintf("http://%v:%v/code/php/getInstalledBuckets.php", machine, port)

  resp, err := http.Get(url)
  if err != nil {
    println("Error: could not read installed buckets on", machine, ":", port)
  }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    println("Error: could not read response from machine")
  }

  var f []Process
  err = json.Unmarshal(body[:len(body)], &f)
  if err != nil {
    fmt.Printf("%v\n%s\n\n", err, body)
  }

  for _, vv := range f {
    vv.Machine = machine
    vv.Port = port

    // check if we have a filter argument
    if filter != nil {
      if filter.MatchString(vv.Machine) {
        results = append(results, vv)
        break                    
      }
      if filter.MatchString(vv.Port) {
        results = append(results, vv)
        break                    
      }
      if filter.MatchString(vv.AETitle) {
        results = append(results, vv)
        break                    
      }
      if filter.MatchString(vv.Description) {
        results = append(results, vv)
        break                    
      }
      if filter.MatchString(vv.License) {
        results = append(results, vv)
        break                    
      }
      if filter.MatchString(vv.Name) {
        results = append(results, vv)
        break                    
      }
      if filter.MatchString(vv.Version) {
        results = append(results, vv)
        break                    
      }
    } else { // by default add all
      results = append(results, vv)                
    }
  }
  return results
}


func main() {
     rand.Seed(time.Now().UTC().UnixNano())

     app := cli.NewApp()
     app.Name = "mb"
     app.Usage = "MagickBox command shell for query, send, retrieve, and deletion of data.\n\n" +
                 "   Start by listing known MagickBox instances (queryMachines). Identify your machines\n" +
                 "   and add them using 'activeMachines add'. They will be used for all future commands.\n\n" +
                 "   Also add your own identity using the setSender command. These steps need to be done only once.\n\n" +
                 "   Most calls return textual output in JSON format that can be processed by tools\n" +
                 "   such as jq (http://stedolan.github.io/jq/).\n\n" +
                 "   Regular expressions are used to identify individual sessions. They are applied\n" +
                 "   to all field values returned by the list command. If a session matches, the\n" +
                 "   command will be applied to it (list, push, pull, remove).\n\n" +
                 "   If data is send (push) and there is more than 1 machine available that provide that processing\n" +
                 "   one of them will be selected by random. Commands such as 'list' will return the machine and port\n" +
                 "   used for that session."
     app.Version = "0.0.2"
     app.Author = "Hauke Bartsch"
     app.Email = "HaukeBartsch@gmail.com"
     app.Flags = []cli.Flag {
      cli.StringFlag {
        Name: "config-sender",
        Value: "",
        Usage: "Identify yourself, value is used as AETitleCaller [--config-sender <string>]",
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
        Usage:     "Send a directory for processing [push <aetitle> <dicom directory> [<arguments>]]",
        Description: "Send a directory with DICOM data to the MagickBox for processing.\n" +
                     "   The <aetitle> is used to specify what type of processing will be run.\n\n" +
                     "   Example:\n" +
                     "   > mb push ProcFS53 /space/data/DICOMS/Subj001/\n" +
                     "   Sends the specified directory for FreeSurfer processing to the currently defined default MB instance.\n",
        Action: func(c *cli.Context) {
          if len(c.Args()) < 2 {
            fmt.Printf("Error: don't know what to send (usage: <aetitle> <directory to send> [<arguments>])\n")
          } else {
            if len(c.Args()) == 2 {
               sendJob( c.Args()[0], c.Args()[1], "" )
            } else {
               sendJob( c.Args()[0], c.Args()[1], c.Args()[2] )              
            }
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
            f := getMagickBoxes()
            printListOfMagickBoxes( f )
        },
     },
/*     {
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
     }, */
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
     {
        Name:      "computeModules",
        ShortName: "c",
        Usage:      "Get list of buckets for the current machine",
        Action: func(c *cli.Context) {
          var ms []Machine = getActiveMagickBoxes()

          var filter *regexp.Regexp = nil
          if len(c.Args()) == 1 {
            filter = regexp.MustCompile(c.Args()[0])
          }

          var results []Process
          for _, v := range ms {
          
            machine := v.Machine
            port := v.Port

            var res = getInstalledBuckets(machine, port, filter)
            results = append(results, res...)
          }

          b, err := json.MarshalIndent(results, "", "  ")
          if err != nil {
              fmt.Println("error:", err)
           }
           fmt.Printf("%v\n", string(b))
        },
     },
     {
        Name:      "activeMachines",
        ShortName: "a",
        Usage:      "Get list of active magick box machines",
        Description: "Without an argument returns list of active magick box machines.\n" +
                     "   use 'add <ip> <port>' and 'rm <ip> <port>' to add or remove machines.",
        Action: func(c *cli.Context) {

          // read from local store
          var ms []Machine = getActiveMagickBoxes()

          if len(c.Args()) > 0 {
            if len(c.Args()) == 3 {
              switch c.Args()[0] {
                case "add":
                  // find out if that entry is already part of the active MBs
                  for _, v := range ms {
                     if (v.Machine == c.Args()[1]) && (v.Port == c.Args()[2]) {
                        fmt.Println("info: machine already part of the active list, nothing will be done")
                        return
                     }
                  }

                  ms = append(ms, Machine{Machine: c.Args()[1], Port: c.Args()[2]})
                  saveActiveMagickBoxes( ms )
                case "rm":
                  fmt.Println("rm here")
                  // find out if that entry is part of the active MBs
                  var idx = -1
                  for k, v := range ms {
                     if (v.Machine == c.Args()[1]) && (v.Port == c.Args()[2]) {
                       idx = k
                       break
                     }
                  }
                  if idx == -1 {
                     fmt.Println("Error: machine not part of activeMachines")
                     return
                  }
                  ms = append(ms[:idx], ms[idx+1:]...)
                  saveActiveMagickBoxes( ms )
                default:
                  if (c.Args()[0] != "add") && (c.Args()[0] != "rm") {
                    fmt.Println("Error: only add and rm are supported")
                    return
                  }
              }
            } else {
              fmt.Println("error: unknown arguments to activeMachines")
              return
            }
          }

          // print out the result
          b, err := json.MarshalIndent(ms, "", "  ")
          if err != nil {
            fmt.Println("error:", err)
          }
          fmt.Printf("%v\n", string(b))

        },
     },
  }

  app.Run(os.Args)
}