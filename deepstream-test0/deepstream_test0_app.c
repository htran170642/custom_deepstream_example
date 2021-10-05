/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#include <gst/gst.h>
#include <glib.h>
#include <stdio.h>
#include <math.h>
#include <string.h>
#include <sys/time.h>
#include <cuda_runtime_api.h>
#include <time.h>

#include "gstnvdsmeta.h"
// #include "nvdsmeta_schema.h"
#include "custom_meta_schema.h"
//#include "gstnvstreammeta.h"
#ifndef PLATFORM_TEGRA
#include "gst-nvmessage.h"
#endif

#define MAX_DISPLAY_LEN 64
#define MAX_TIME_STAMP_LEN 32

#define PGIE_CLASS_ID_VEHICLE 0
#define PGIE_CLASS_ID_PERSON 2

#define PIPELINE_NAME "pipepline_1"
#define PGIE_CONFIG_FILE  "dstest0_pgie_config.txt"
#define MSCONV_CONFIG_FILE "dstest0_msgconv_config.txt"
#define PROTOCOL_ADAPTOR_LIB "/opt/nvidia/deepstream/deepstream-5.1/lib/libnvds_kafka_proto.so"
#define CONNECTION_STRING "10.208.208.167;9092"
#define CONFIG_FILE_PATH "cfg_kafka.txt"
#define TOPIC "ds19"
#define SCHEMA_TYPE 0

/* By default, OSD process-mode is set to CPU_MODE. To change mode, set as:
 * 1: GPU mode (for Tesla only)
 * 2: HW mode (For Jetson only)
 */
#define OSD_PROCESS_MODE 0

/* By default, OSD will not display text. To display text, change this to 1 */
#define OSD_DISPLAY_TEXT 1

/* The muxer output resolution must be set if the input streams will be of
 * different resolution. The muxer will scale all the input frames to this
 * resolution. */
#define MUXER_OUTPUT_WIDTH 1920
#define MUXER_OUTPUT_HEIGHT 1080

/* Muxer batch formation timeout, for e.g. 40 millisec. Should ideally be set
 * based on the fastest source's framerate. */
#define MUXER_BATCH_TIMEOUT_USEC 40000

#define TILED_OUTPUT_WIDTH 1280
#define TILED_OUTPUT_HEIGHT 720

/* NVIDIA Decoder source pad memory feature. This feature signifies that source
 * pads having this capability will push GstBuffers containing cuda buffers. */
#define GST_CAPS_FEATURES_NVMM "memory:NVMM"

static gboolean display_off = FALSE;
gint frame_number = 0;
gchar pgie_classes_str[4][32] = { "Vehicle", "TwoWheeler", "Person",
  "RoadSign"
};

#define FPS_PRINT_INTERVAL 300
//static struct timeval start_time = { };

//static guint probe_counter = 0;


static void generate_ts_rfc3339 (char *buf, int buf_size)
{
  time_t tloc;
  struct tm tm_log;
  struct timespec ts;
  char strmsec[6]; //.nnnZ\0

  clock_gettime(CLOCK_REALTIME,  &ts);
  memcpy(&tloc, (void *)(&ts.tv_sec), sizeof(time_t));
  gmtime_r(&tloc, &tm_log);
  strftime(buf, buf_size,"%Y-%m-%dT%H:%M:%S", &tm_log);
  int ms = ts.tv_nsec/1000000;
  g_snprintf(strmsec, sizeof(strmsec),".%.3dZ", ms);
  strncat(buf, strmsec, buf_size);
}

static gpointer meta_copy_func (gpointer data, gpointer user_data){
  NvDsUserMeta *user_meta = (NvDsUserMeta *) data;
  NvDsEventMsgMeta *srcMeta = (NvDsEventMsgMeta *) user_meta->user_meta_data;
  NvDsFrameObjDescEvent *srcExt = (NvDsFrameObjDescEvent *) srcMeta->extMsg;

  NvDsEventMsgMeta *dstMeta = NULL;
  NvDsFrameObjDescEvent *dstExt = NULL;

  dstMeta = (NvDsEventMsgMeta*)g_memdup (srcMeta, sizeof(NvDsEventMsgMeta));
  dstMeta->sensorStr = g_strdup(srcMeta->sensorStr);

  if(srcMeta->extMsgSize > 0){
    dstMeta->extMsg = g_memdup(srcExt, sizeof(NvDsFrameObjDescEvent));
    dstExt = (NvDsFrameObjDescEvent *)dstMeta->extMsg;

    if (srcExt->sourceUri){
      dstExt->sourceUri = g_strdup (srcExt->sourceUri);
    }
    if(srcExt->filterCloudModules){
      dstExt->filterCloudModules = g_strdup (srcExt->filterCloudModules);
    }
    if(srcExt->sourceCloudModules){
      dstExt->sourceCloudModules = g_strdup(srcExt->sourceCloudModules);
    }
  }

  if (srcMeta->ts){
    dstMeta->ts = g_strdup (srcMeta->ts);
  }
  return dstMeta;
}

static void meta_free_func(gpointer data, gpointer user_data){
  NvDsUserMeta *user_meta = (NvDsUserMeta *) data;
  NvDsEventMsgMeta *meta = (NvDsEventMsgMeta *) user_meta->user_meta_data;

  if(meta->ts){
    g_free (meta->ts);
  }

  if(meta->sensorStr){
    g_free(meta->sensorStr);
  }
  if(meta->extMsgSize > 0){
    NvDsFrameObjDescEvent *srcExt = (NvDsFrameObjDescEvent *) meta->extMsg;
    if(srcExt->sourceUri){
      g_free (srcExt->sourceUri);
    }
    if(srcExt->filterCloudModules){
      g_free(srcExt->filterCloudModules);
    }
    if(srcExt->sourceCloudModules){
      g_free(srcExt->sourceCloudModules);
    }
    g_free(meta->extMsg);
  }
  meta->extMsg = NULL;

  g_free(user_meta->user_meta_data);
  user_meta->user_meta_data = NULL;
}

void
generate_object_event_msg_meta( gpointer data, NvDsFrameMeta* frame_meta){
  NvDsEventMsgMeta *meta = (NvDsEventMsgMeta *) data;
  NvDsFrameObjDescEvent* frame_obj_desc = (NvDsFrameObjDescEvent*)meta->extMsg;
  // NvDsSourceConfigExt source_config = appCtx->config.multi_source_config[frame_meta->source_id];

  meta->sensorId = frame_meta->source_id;
  // meta->sensorStr = g_strdup(source_config.uri);
  meta->sensorStr = g_strdup ("sensor-0");
  meta->ts = (gchar *) g_malloc0 (MAX_TIME_STAMP_LEN + 1);
  // if(source_config.type == NV_DS_SOURCE_URI){
  //   meta->otherAttrs = (gchar*)"file";
  // }else if(source_config.type == NV_DS_SOURCE_RTSP || source_config.type == NV_DS_SOURCE_CAMERA_V4L2){
  //   meta->otherAttrs = (gchar*)"camera";
  // }

  generate_ts_rfc3339(meta->ts, MAX_TIME_STAMP_LEN);

  // frame_obj_desc->filterCloudModules = 
  //     g_strjoin(";", FIGHT_MODULE_NAME, WEAPON_MODULE_NAME, DRUNK_MODULE_NAME, NULL);
  
  frame_obj_desc->sourceId = frame_meta->source_id;
  // frame_obj_desc->sourceType = source_config.type;
  frame_obj_desc->frameHeight = frame_meta->source_frame_height;
  frame_obj_desc->frameWidth = frame_meta->source_frame_width;

  // frame_obj_desc->sourceUri = g_strdup(source_config.uri);
  // frame_obj_desc->sourceCloudModules = g_strdup(source_config.cloud_modules);
}


/* tiler_sink_pad_buffer_probe  will extract metadata received on OSD sink pad
 * and update params for drawing rectangle, object information etc. */

static GstPadProbeReturn
tiler_src_pad_buffer_probe (GstPad * pad, GstPadProbeInfo * info,
    gpointer u_data)
{
    GstBuffer *buf = (GstBuffer *) info->data;
    guint num_rects = 0; 
    NvDsObjectMeta *obj_meta = NULL;
    guint vehicle_count = 0;
    guint person_count = 0;
    gboolean is_first_object = TRUE;
    NvDsMetaList * l_frame = NULL;
    NvDsMetaList * l_obj = NULL;
    NvDsFrameObjDescEvent* frame_obj_desc = NULL;

    NvDsBatchMeta *batch_meta = gst_buffer_get_nvds_batch_meta (buf);
    if (!batch_meta) {
      return GST_PAD_PROBE_OK;
    }
    for (l_frame = batch_meta->frame_meta_list; l_frame != NULL; l_frame = l_frame->next) {
      NvDsFrameMeta *frame_meta = (NvDsFrameMeta *) (l_frame->data);
      if (frame_meta == NULL) {
        continue;
      }

      is_first_object = TRUE;
      for (l_obj = frame_meta->obj_meta_list; l_obj != NULL; l_obj = l_obj->next) {
        obj_meta = (NvDsObjectMeta *) (l_obj->data);
        if (obj_meta == NULL) {
          continue;
        }
        if (obj_meta->class_id == PGIE_CLASS_ID_VEHICLE) {
            vehicle_count++;
            num_rects++;
        }
        if (obj_meta->class_id == PGIE_CLASS_ID_PERSON) {
            person_count++;
            num_rects++;
        }
        
        if(frame_obj_desc == NULL){
          frame_obj_desc = (NvDsFrameObjDescEvent*)g_malloc0(sizeof(NvDsFrameObjDescEvent));
          frame_obj_desc->objCounts = 0;
        }
        if (num_rects < MAX_OBJ_NUM) {
          /* Frequency of messages to be send will be based on use case.
          * Here message is being sent for first object every 30 frames.
          */
          if (obj_meta->class_id == PGIE_CLASS_ID_VEHICLE)
            frame_obj_desc->objMetaList[frame_obj_desc->objCounts].objType = NVDS_OBJECT_TYPE_VEHICLE;
          else
            frame_obj_desc->objMetaList[frame_obj_desc->objCounts].objType = NVDS_OBJECT_TYPE_PERSON;
          strncpy(frame_obj_desc->objMetaList[frame_obj_desc->objCounts].label, obj_meta->obj_label, MAX_LABEL_SIZE);

          frame_obj_desc->objMetaList[frame_obj_desc->objCounts].bbox.top = obj_meta->rect_params.top;
          frame_obj_desc->objMetaList[frame_obj_desc->objCounts].bbox.left = obj_meta->rect_params.left;
          frame_obj_desc->objMetaList[frame_obj_desc->objCounts].bbox.width = obj_meta->rect_params.width;
          frame_obj_desc->objMetaList[frame_obj_desc->objCounts].bbox.height = obj_meta->rect_params.height;
          frame_obj_desc->objMetaList[frame_obj_desc->objCounts].trackingId = obj_meta->object_id;
          frame_obj_desc->objMetaList[frame_obj_desc->objCounts].confidence = obj_meta->confidence;
          frame_obj_desc->objCounts += 1;
        }
      }

      if (is_first_object && frame_obj_desc != NULL && frame_number %30 == 0) {
        NvDsEventMsgMeta *msg_meta = (NvDsEventMsgMeta *) g_malloc0 (sizeof (NvDsEventMsgMeta));
        msg_meta->type = NVDS_EVENT_CUSTOM;
        frame_obj_desc->frameId = frame_number;
        msg_meta->frameId = frame_number;
        msg_meta->extMsg = frame_obj_desc;
        msg_meta->extMsgSize = sizeof (NvDsFrameObjDescEvent);
        generate_object_event_msg_meta(msg_meta, frame_meta);
        
        NvDsUserMeta *user_event_meta = nvds_acquire_user_meta_from_pool (batch_meta);
        if (user_event_meta) {
          user_event_meta->user_meta_data = (void *) msg_meta;
          user_event_meta->base_meta.meta_type = NVDS_EVENT_MSG_META;
          user_event_meta->base_meta.copy_func = (NvDsMetaCopyFunc) meta_copy_func;
          user_event_meta->base_meta.release_func = (NvDsMetaReleaseFunc) meta_free_func;
          nvds_add_user_meta_to_frame(frame_meta, user_event_meta);
          is_first_object = FALSE;
          frame_obj_desc = NULL;
        } else {
          g_print ("Error in attaching event meta to buffer\n");
        }
      }
    }
    g_print ("Frame Number = %d Number of objects = %d "
          "Vehicle Count = %d Person Count = %d\n",
          frame_number, num_rects, vehicle_count, person_count);
    frame_number++;
    return GST_PAD_PROBE_OK;
}

static gboolean
bus_call (GstBus * bus, GstMessage * msg, gpointer data)
{
  GMainLoop *loop = (GMainLoop *) data;
  switch (GST_MESSAGE_TYPE (msg)) {
    case GST_MESSAGE_EOS:
      g_print ("End of stream\n");
      g_main_loop_quit (loop);
      break;
    case GST_MESSAGE_WARNING:
    {
      gchar *debug;
      GError *error;
      gst_message_parse_warning (msg, &error, &debug);
      g_printerr ("WARNING from element %s: %s\n",
          GST_OBJECT_NAME (msg->src), error->message);
      g_free (debug);
      g_printerr ("Warning: %s\n", error->message);
      g_error_free (error);
      break;
    }
    case GST_MESSAGE_ERROR:
    {
      gchar *debug;
      GError *error;
      gst_message_parse_error (msg, &error, &debug);
      g_printerr ("ERROR from element %s: %s\n",
          GST_OBJECT_NAME (msg->src), error->message);
      if (debug)
        g_printerr ("Error details: %s\n", debug);
      g_free (debug);
      g_error_free (error);
      g_main_loop_quit (loop);
      break;
    }
#ifndef PLATFORM_TEGRA
    case GST_MESSAGE_ELEMENT:
    {
      if (gst_nvmessage_is_stream_eos (msg)) {
        guint stream_id;
        if (gst_nvmessage_parse_stream_eos (msg, &stream_id)) {
          g_print ("Got EOS from stream %d\n", stream_id);
        }
      }
      break;
    }
#endif
    default:
      break;
  }
  return TRUE;
}

static void
cb_newpad (GstElement * decodebin, GstPad * decoder_src_pad, gpointer data)
{
  g_print ("In cb_newpad\n");
  GstCaps *caps = gst_pad_get_current_caps (decoder_src_pad);
  const GstStructure *str = gst_caps_get_structure (caps, 0);
  const gchar *name = gst_structure_get_name (str);
  GstElement *source_bin = (GstElement *) data;
  GstCapsFeatures *features = gst_caps_get_features (caps, 0);

  /* Need to check if the pad created by the decodebin is for video and not
   * audio. */
  if (!strncmp (name, "video", 5)) {
    /* Link the decodebin pad only if decodebin has picked nvidia
     * decoder plugin nvdec_*. We do this by checking if the pad caps contain
     * NVMM memory features. */
    if (gst_caps_features_contains (features, GST_CAPS_FEATURES_NVMM)) {
      /* Get the source bin ghost pad */
      GstPad *bin_ghost_pad = gst_element_get_static_pad (source_bin, "src");
      if (!gst_ghost_pad_set_target (GST_GHOST_PAD (bin_ghost_pad),
              decoder_src_pad)) {
        g_printerr ("Failed to link decoder src pad to source bin ghost pad\n");
      }
      gst_object_unref (bin_ghost_pad);
    } else {
      g_printerr ("Error: Decodebin did not pick nvidia decoder plugin.\n");
    }
  }
}

static void
decodebin_child_added (GstChildProxy * child_proxy, GObject * object,
    gchar * name, gpointer user_data)
{
  g_print ("Decodebin child added: %s\n", name);
  if (g_strrstr (name, "decodebin") == name) {
    g_signal_connect (G_OBJECT (object), "child-added",
        G_CALLBACK (decodebin_child_added), user_data);
  }
}

static GstElement *
create_source_bin (guint index, gchar * uri)
{
  GstElement *bin = NULL, *uri_decode_bin = NULL;
  gchar bin_name[16] = { };

  g_snprintf (bin_name, 15, "source-bin-%02d", index);
  /* Create a source GstBin to abstract this bin's content from the rest of the
   * pipeline */
  bin = gst_bin_new (bin_name);

  /* Source element for reading from the uri.
   * We will use decodebin and let it figure out the container format of the
   * stream and the codec and plug the appropriate demux and decode plugins. */
  uri_decode_bin = gst_element_factory_make ("uridecodebin", "uri-decode-bin");

  if (!bin || !uri_decode_bin) {
    g_printerr ("One element in source bin could not be created.\n");
    return NULL;
  }

  /* We set the input uri to the source element */
  g_object_set (G_OBJECT (uri_decode_bin), "uri", uri, NULL);

  /* Connect to the "pad-added" signal of the decodebin which generates a
   * callback once a new pad for raw data has beed created by the decodebin */
  g_signal_connect (G_OBJECT (uri_decode_bin), "pad-added",
      G_CALLBACK (cb_newpad), bin);
  g_signal_connect (G_OBJECT (uri_decode_bin), "child-added",
      G_CALLBACK (decodebin_child_added), bin);

  gst_bin_add (GST_BIN (bin), uri_decode_bin);

  /* We need to create a ghost pad for the source bin which will act as a proxy
   * for the video decoder src pad. The ghost pad will not have a target right
   * now. Once the decode bin creates the video decoder and generates the
   * cb_newpad callback, we will set the ghost pad target to the video decoder
   * src pad. */
  if (!gst_element_add_pad (bin, gst_ghost_pad_new_no_target ("src",
              GST_PAD_SRC))) {
    g_printerr ("Failed to add ghost pad in source bin\n");
    return NULL;
  }

  return bin;
}

int
main (int argc, char *argv[])
{
  GMainLoop *loop = NULL;
  GstElement *pipeline = NULL, *streammux = NULL, *sink = NULL, *pgie = NULL,
      *queue1, *queue2, *queue3, *queue4, *queue5, *queue6, *nvvidconv = NULL,
      *nvosd = NULL, *tiler = NULL;
  GstElement *msgconv = NULL, *msgbroker = NULL, *tee = NULL;
  GstElement *transform = NULL;
  GstBus *bus = NULL;
  guint bus_watch_id;
  GstPad *tiler_src_pad = NULL;
  GstPad *tee_render_pad = NULL;
  GstPad *tee_msg_pad = NULL;
  GstPad *sink_pad = NULL;
  GstPad *src_pad = NULL;
  guint i, num_sources;
  guint tiler_rows, tiler_columns;
  guint pgie_batch_size;

  int current_device = -1;
  cudaGetDevice(&current_device);
  struct cudaDeviceProp prop;
  cudaGetDeviceProperties(&prop, current_device);

  /* Check input arguments */
  if (argc < 2) {
    g_printerr ("Usage: %s <uri1> [uri2] ... [uriN] \n", argv[0]);
    return -1;
  }
  num_sources = argc - 1;

  /* Standard GStreamer initialization */
  gst_init (&argc, &argv);
  loop = g_main_loop_new (NULL, FALSE);

  /* Create gstreamer elements */
  /* Create Pipeline element that will form a connection of other elements */
  pipeline = gst_pipeline_new (PIPELINE_NAME);

  /* Create nvstreammux instance to form batches from one or more sources. */
  streammux = gst_element_factory_make ("nvstreammux", "stream-muxer");

  if (!pipeline || !streammux) {
    g_printerr ("One element could not be created. Exiting.\n");
    return -1;
  }
  gst_bin_add (GST_BIN (pipeline), streammux);

  for (i = 0; i < num_sources; i++) {
    GstPad *sinkpad, *srcpad;
    gchar pad_name[16] = { };
    GstElement *source_bin = create_source_bin (i, argv[i + 1]);

    if (!source_bin) {
      g_printerr ("Failed to create source bin. Exiting.\n");
      return -1;
    }

    gst_bin_add (GST_BIN (pipeline), source_bin);

    g_snprintf (pad_name, 15, "sink_%u", i);
    sinkpad = gst_element_get_request_pad (streammux, pad_name);
    if (!sinkpad) {
      g_printerr ("Streammux request sink pad failed. Exiting.\n");
      return -1;
    }

    srcpad = gst_element_get_static_pad (source_bin, "src");
    if (!srcpad) {
      g_printerr ("Failed to get src pad of source bin. Exiting.\n");
      return -1;
    }

    if (gst_pad_link (srcpad, sinkpad) != GST_PAD_LINK_OK) {
      g_printerr ("Failed to link source bin to stream muxer. Exiting.\n");
      return -1;
    }

    gst_object_unref (srcpad);
    gst_object_unref (sinkpad);
  }

  /* Use nvinfer to infer on batched frame. */
  pgie = gst_element_factory_make ("nvinfer", "primary-nvinference-engine");

  /* Add queue elements between every two elements */
  queue1 = gst_element_factory_make ("queue", "queue1");
  queue2 = gst_element_factory_make ("queue", "queue2");
  queue3 = gst_element_factory_make ("queue", "queue3");
  queue4 = gst_element_factory_make ("queue", "queue4");
  queue5 = gst_element_factory_make ("queue", "queue5");
  queue6 = gst_element_factory_make ("queue", "queue6");

  /* Use nvtiler to composite the batched frames into a 2D tiled array based
   * on the source of the frames. */
  tiler = gst_element_factory_make ("nvmultistreamtiler", "nvtiler");

  /* Use convertor to convert from NV12 to RGBA as required by nvosd */
  nvvidconv = gst_element_factory_make ("nvvideoconvert", "nvvideo-converter");

  /* Create OSD to draw on the converted RGBA buffer */
  nvosd = gst_element_factory_make ("nvdsosd", "nv-onscreendisplay");

   /* Create msg converter to generate payload from buffer metadata */
  msgconv = gst_element_factory_make ("nvmsgconv", "nvmsg-converter");

  /* Create msg broker to send payload to server */
  msgbroker = gst_element_factory_make ("nvmsgbroker", "nvmsg-broker");

  /* Create tee to render buffer and send message simultaneously*/
  tee = gst_element_factory_make ("tee", "nvsink-tee");

  /* Finally render the osd output */
  if(prop.integrated) {
    transform = gst_element_factory_make ("nvegltransform", "nvegl-transform");
  }
  // sink = gst_element_factory_make ("nveglglessink", "nvvideo-renderer");
  sink = gst_element_factory_make ("fakesink", "nvvideo-renderer");

  if (!pgie || !tiler || !nvvidconv || !nvosd || !msgconv || !msgbroker || !tee || !sink) {
    g_printerr ("One element could not be created. Exiting.\n");
    return -1;
  }

  if(!transform && prop.integrated) {
    g_printerr ("One tegra element could not be created. Exiting.\n");
    return -1;
  }

  g_object_set (G_OBJECT (streammux), "batch-size", num_sources, NULL);

  g_object_set (G_OBJECT (streammux), "width", MUXER_OUTPUT_WIDTH, "height",
      MUXER_OUTPUT_HEIGHT,
      "batched-push-timeout", MUXER_BATCH_TIMEOUT_USEC, NULL);

  /* Configure the nvinfer element using the nvinfer config file. */
  g_object_set (G_OBJECT (pgie),
      "config-file-path", PGIE_CONFIG_FILE, NULL);

  /* Override the batch-size set in the config file with the number of sources. */
  g_object_get (G_OBJECT (pgie), "batch-size", &pgie_batch_size, NULL);
  if (pgie_batch_size != num_sources) {
    g_printerr
        ("WARNING: Overriding infer-config batch-size (%d) with number of sources (%d)\n",
        pgie_batch_size, num_sources);
    g_object_set (G_OBJECT (pgie), "batch-size", num_sources, NULL);
  }

  tiler_rows = (guint) sqrt (num_sources);
  tiler_columns = (guint) ceil (1.0 * num_sources / tiler_rows);
  /* we set the tiler properties here */
  g_object_set (G_OBJECT (tiler), "rows", tiler_rows, "columns", tiler_columns,
      "width", TILED_OUTPUT_WIDTH, "height", TILED_OUTPUT_HEIGHT, NULL);

  g_object_set (G_OBJECT (nvosd), "process-mode", OSD_PROCESS_MODE,
      "display-text", OSD_DISPLAY_TEXT, NULL);

  g_object_set (G_OBJECT(msgconv), "config", MSCONV_CONFIG_FILE, NULL);
  g_object_set (G_OBJECT(msgconv), "payload-type", SCHEMA_TYPE, NULL);

  g_object_set (G_OBJECT(msgbroker), "proto-lib", PROTOCOL_ADAPTOR_LIB,
                "conn-str", CONNECTION_STRING, "sync", FALSE, NULL);

  g_object_set (G_OBJECT(msgbroker), "topic", TOPIC, NULL);

  g_object_set (G_OBJECT(msgbroker), "config", CONFIG_FILE_PATH, NULL);

  g_object_set (G_OBJECT (sink), "sync", TRUE, NULL);
  g_print ("before init pipeline\n");
  /* we add a message handler */
  bus = gst_pipeline_get_bus (GST_PIPELINE (pipeline));
  bus_watch_id = gst_bus_add_watch (bus, bus_call, loop);
  gst_object_unref (bus);
  g_print ("End initializing pipeline\n");
  
  g_print ("Adding all elements to bin\n");
  gst_bin_add_many (GST_BIN (pipeline), queue1, pgie, queue2, tiler, queue3,
      nvvidconv, queue4, nvosd, tee, queue5, msgconv, msgbroker, queue6, sink, NULL);
  g_print ("End bin added\n");

  if(prop.integrated) {
    if (!display_off)
      gst_bin_add (GST_BIN (pipeline), transform);
  }
 
  g_print ("Link 1\n");
  if (!gst_element_link_many (streammux, queue1, pgie, queue2, tiler, queue3,
       nvvidconv, queue4, nvosd, tee, NULL)) {
    g_printerr ("1. Elements could not be linked. Exiting.\n");
    return -1;
  }
  g_print ("Link 2\n");
  if (!gst_element_link_many (queue5, msgconv, msgbroker, NULL)) {
    g_printerr ("2. Elements could not be linked. Exiting.\n");
    return -1;
  }

  g_print ("Link 3\n");
  if(prop.integrated) {
    if (!display_off) {
      if (!gst_element_link_many (queue6, transform, sink, NULL)) {
        g_printerr ("3. Elements could not be linked. Exiting.\n");
        return -1;
      }
    } else {
      if (!gst_element_link (queue6, sink)) {
        g_printerr ("3. Elements could not be linked. Exiting.\n");
        return -1;
      }
    }
  }
  else {
    if (!gst_element_link (queue6, sink)) {
      g_printerr ("4. Elements could not be linked. Exiting.\n");
      return -1;
    }
  }

  g_print ("End link\n");
  sink_pad = gst_element_get_static_pad (queue5, "sink");
  tee_msg_pad = gst_element_get_request_pad (tee, "src_%u");
  tee_render_pad = gst_element_get_request_pad (tee, "src_%u");
  if (!tee_msg_pad || !tee_render_pad) {
    g_printerr ("Unable to get request pads\n");
    return -1;
  }

  if (gst_pad_link (tee_msg_pad, sink_pad) != GST_PAD_LINK_OK) {
    g_printerr ("Unable to link tee and message converter\n");
    gst_object_unref (sink_pad);
    return -1;
  }

  gst_object_unref (sink_pad);

  sink_pad = gst_element_get_static_pad (queue6, "sink");
  if (gst_pad_link (tee_render_pad, sink_pad) != GST_PAD_LINK_OK) {
    g_printerr ("Unable to link tee and render\n");
    gst_object_unref (sink_pad);
    return -1;
  }

  gst_object_unref (sink_pad);

  /* Lets add probe to get informed of the meta data generated, we add probe to
   * the sink pad of the osd element, since by that time, the buffer would have
   * had got all the metadata. */
  tiler_src_pad = gst_element_get_static_pad (pgie, "src");
  if (!tiler_src_pad)
    g_print ("Unable to get src pad\n");
  else
    g_print ("Getting src pad\n");
    gst_pad_add_probe (tiler_src_pad, GST_PAD_PROBE_TYPE_BUFFER,
        tiler_src_pad_buffer_probe, NULL, NULL);
  gst_object_unref (tiler_src_pad);

  /* Set the pipeline to "playing" state */
  g_print ("Now playing:");
  for (i = 0; i < num_sources; i++) {
    g_print (" %s,", argv[i + 1]);
  }
  g_print ("\n");
  gst_element_set_state (pipeline, GST_STATE_PLAYING);

  /* Wait till pipeline encounters an error or EOS */
  g_print ("Running...\n");
  g_main_loop_run (loop);

  /* Out of the main loop, clean up nicely */
  g_print ("Returned, stopping playback\n");

  /* Release the request pads from the tee, and unref them */
  gst_element_release_request_pad (tee, tee_msg_pad);
  gst_element_release_request_pad (tee, tee_render_pad);
  gst_object_unref (tee_msg_pad);
  gst_object_unref (tee_render_pad);

  gst_element_set_state (pipeline, GST_STATE_NULL);
  g_print ("Deleting pipeline\n");
  gst_object_unref (GST_OBJECT (pipeline));
  g_source_remove (bus_watch_id);
  g_main_loop_unref (loop);
  return 0;
}
