You must have the following development packages installed
   GStreamer-1.0
   GStreamer-1.0 Base Plugins
   GStreamer-1.0 gstrtspserver
   X11 client-side library

To install these packages, execute the following command:
```bash
$ sudo apt-get install libgstreamer-plugins-base1.0-dev libgstreamer1.0-dev libgstrtspserver-1.0-dev libx11-dev
```

To compile:
```bash
$ cd apps/deepstream-test0/
$ make
```

To run:
```bash
$ ./deepstream-test0-app <uri1> [uri2] ... [uriN]
```
e.g.
```bash
$ ./deepstream-test0-app file:///opt/nvidia/deepstream/deepstream-5.1/samples/streams/sample_1080p_h264.mp4 
$ ./deepstream-test0-app rtsp://127.0.0.1/video1 rtsp://127.0.0.1/video2
```
