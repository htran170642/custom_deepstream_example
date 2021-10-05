/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION.  All rights reserved.
 *
 * NVIDIA Corporation and its licensors retain all intellectual property
 * and proprietary rights in and to this software, related documentation
 * and any modifications thereto.  Any use, reproduction, disclosure or
 * distribution of this software and related documentation without an express
 * license agreement from NVIDIA Corporation is strictly prohibited.
 *
 */

#include "nvmsgconv.h"
#include <json-glib/json-glib.h>
#include <uuid.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <vector>
#include <unordered_map>

using namespace std;


#define CONFIG_GROUP_SENSOR "sensor"
#define CONFIG_GROUP_PLACE "place"
#define CONFIG_GROUP_ANALYTICS "analytics"

#define CONFIG_KEY_COORDINATE "coordinate"
#define CONFIG_KEY_DESCRIPTION "description"
#define CONFIG_KEY_ENABLE  "enable"
#define CONFIG_KEY_ID "id"
#define CONFIG_KEY_LANE "lane"
#define CONFIG_KEY_LEVEL "level"
#define CONFIG_KEY_LOCATION "location"
#define CONFIG_KEY_NAME "name"
#define CONFIG_KEY_SOURCE "source"
#define CONFIG_KEY_TYPE "type"
#define CONFIG_KEY_VERSION "version"


#define CONFIG_KEY_PLACE_SUB_FIELD1 "place-sub-field1"
#define CONFIG_KEY_PLACE_SUB_FIELD2 "place-sub-field2"
#define CONFIG_KEY_PLACE_SUB_FIELD3 "place-sub-field3"

#define DEFAULT_CSV_FIELDS 10


#define CHECK_ERROR(error) \
    if (error) { \
      cout << "Error: " << error->message << endl; \
      goto done; \
    }

#define MAX_OBJ_NUM 256
#define MAX_LABEL_SIZE 128

typedef struct 
{
  NvDsObjectType objType;
  /** Holds the object's bounding box. */
  NvDsRect bbox;

  /** Holds the confidence level of the inference. */
  gdouble confidence;
  /** Holds the object's tracking ID. */
  gint trackingId;

  gchar label[MAX_LABEL_SIZE];

}NvDsSimpleObjectMeta;


typedef struct
{
  gchar* sourceUri;
  gint sourceId;
  gint sourceType;
  guint frameId;

  guint frameWidth;
  guint frameHeight;

  guint objCounts;
  NvDsSimpleObjectMeta objMetaList[MAX_OBJ_NUM];

  gchar* filterCloudModules;
  gchar* sourceCloudModules;
}NvDsFrameObjDescEvent;

struct NvDsSensorObject {
  string id;
  string type;
  string desc;
};

struct NvDsPayloadPriv {
  unordered_map<int, NvDsSensorObject> sensorObj;
};

static void
get_csv_tokens (const string &text, vector<string> &tokens)
{
  /* This is based on assumption that fields and their locations
   * are fixed in CSV file. This should be updated accordingly if
   * that is not the case.
   */
  gint count = 0;

  gchar **csv_tokens = g_strsplit (text.c_str(), ",", -1);
  gchar **temp = csv_tokens;
  gchar *token;

  while (*temp && count < DEFAULT_CSV_FIELDS) {
    token = *temp++;
    tokens.push_back (string(g_strstrip(token)));
    count++;
  }
  g_strfreev (csv_tokens);
}

static JsonObject*
generate_sensor_object (NvDsMsg2pCtx *ctx, NvDsEventMsgMeta *meta)
{
  NvDsPayloadPriv *privObj = NULL;
  NvDsSensorObject *dsSensorObj = NULL;
  JsonObject *sensorObj;

  privObj = (NvDsPayloadPriv *) ctx->privData;
  auto idMap = privObj->sensorObj.find (meta->sensorId);

  if (idMap != privObj->sensorObj.end()) {
    dsSensorObj = &idMap->second;
  } else {
    cout << "No entry for " CONFIG_GROUP_SENSOR << meta->sensorId
         << " in configuration file" << endl;
    return NULL;
  }

  /* sensor object
   * "sensor": {
       "id": "string",
       "type": "Camera/Puck",
       "description": "Entrance of Endeavor Garage Right Lane"
     }
   */

  // sensor object
  sensorObj = json_object_new ();
  json_object_set_string_member (sensorObj, "id", dsSensorObj->id.c_str());
  json_object_set_string_member (sensorObj, "type", dsSensorObj->type.c_str());
  json_object_set_string_member (sensorObj, "description", dsSensorObj->desc.c_str());

  return sensorObj;
}

static JsonArray*
generate_object_array(NvDsMsg2pCtx *ctx, NvDsFrameObjDescEvent* frame_obj_desc){
  JsonArray *bbox;
  JsonArray *objectArray;
  
  objectArray = json_array_new();
  for(guint idx=0; idx < frame_obj_desc->objCounts; idx++){
    JsonObject* obj = json_object_new();
    bbox = json_array_new();

    json_array_add_double_element(bbox, frame_obj_desc->objMetaList[idx].bbox.top);
    json_array_add_double_element(bbox, frame_obj_desc->objMetaList[idx].bbox.left);
    json_array_add_double_element(bbox, frame_obj_desc->objMetaList[idx].bbox.width);
    json_array_add_double_element(bbox, frame_obj_desc->objMetaList[idx].bbox.height);

    json_object_set_int_member(obj, "trackingId", frame_obj_desc->objMetaList[idx].trackingId);
    json_object_set_array_member(obj, "bbox", bbox);
    json_object_set_string_member(obj, "type", frame_obj_desc->objMetaList[idx].label);

    json_array_add_object_element(objectArray, obj);
  }
  
  return objectArray;
}

static JsonObject*
generate_frame_meta(NvDsMsg2pCtx *ctx, NvDsFrameObjDescEvent* frame_obj_desc){
  JsonObject* frameObj = NULL;
  frameObj = json_object_new();
  json_object_set_int_member(frameObj, "width", frame_obj_desc->frameWidth);
  json_object_set_int_member(frameObj, "height", frame_obj_desc->frameHeight);
  json_object_set_int_member(frameObj, "frameId", frame_obj_desc->frameId);

  return frameObj;
}

static gchar*
generate_schema_message (NvDsMsg2pCtx *ctx, NvDsEventMsgMeta *meta){
  JsonNode *rootNode;
  JsonObject *rootObj;
  JsonObject *sensorObj;

  uuid_t msgId;
  gchar msgIdStr[37];
  JsonArray *objectArray;
  // JsonArray *cloudModules;
  // JsonArray *supportModules;
  // gchar** cloudModulesList;
  // gchar** supportModulesList;
  JsonObject* frameObj;
  gchar* message = NULL;
 

  // TODO: hash sensorObj.id
  // json_object_set_string_member(rootObj, "id", sensorObj.id.c_str());
  // partition-key, follow this guide https://docs.nvidia.com/metropolis/deepstream/dev-guide/text/DS_plugin_gst-nvmsgbroker.html

  if(meta->extMsgSize > 0){
    NvDsFrameObjDescEvent* frame_object_desc = (NvDsFrameObjDescEvent*)meta->extMsg;

    if(frame_object_desc->objCounts > 0){
      sensorObj = generate_sensor_object (ctx, meta);
      if(sensorObj == NULL){
        return NULL;
      }
      objectArray = generate_object_array (ctx, frame_object_desc);
      frameObj = generate_frame_meta(ctx, frame_object_desc);

      uuid_generate_random (msgId);
      uuid_unparse_lower(msgId, msgIdStr);
      rootObj = json_object_new(); 
      
      json_object_set_string_member (rootObj, "messageid", msgIdStr);
      json_object_set_string_member (rootObj, "mdsversion", "1.0");
      json_object_set_string_member (rootObj, "@timestamp", meta->ts);
      json_object_set_object_member (rootObj, "sensor", sensorObj);
      json_object_set_array_member (rootObj, "objects", objectArray);
      json_object_set_object_member (rootObj, "frame", frameObj);

      // supportModulesList = g_strsplit(frame_object_desc->filterCloudModules, ";", 128);
      // cloudModulesList = g_strsplit(frame_object_desc->sourceCloudModules, ";", 128);

      // cloudModules = json_array_new();
      // supportModules = json_array_new();
      // for(guint idx=0; idx < g_strv_length(supportModulesList); idx++){
      //   json_array_add_string_element(supportModules, supportModulesList[idx]);
      // }
      // for(guint idx=0; idx < g_strv_length(cloudModulesList); idx++){
      //   json_array_add_string_element(cloudModules, cloudModulesList[idx]);
      // }

      // json_object_set_array_member (rootObj, "support-modules", supportModules);
      // json_object_set_array_member (rootObj, "source-modules", cloudModules);

      // g_strfreev(supportModulesList);
      // g_strfreev(cloudModulesList);

      rootNode = json_node_new (JSON_NODE_OBJECT);
      json_node_set_object (rootNode, rootObj);
      message = json_to_string (rootNode, TRUE);
      #ifdef NDEBUG
      NVGSTDS_INFO_MSG_V("%s: %s", __func__, message);
      #endif
    }
    if(rootNode != NULL){
      json_node_free(rootNode);
    }
    json_object_unref(rootObj);
  }
  
  return message;
}

static const gchar*
object_enum_to_str (NvDsObjectType type, gchar* objectId)
{
  switch (type) {
    case NVDS_OBJECT_TYPE_VEHICLE:
      return "Vehicle";
    case NVDS_OBJECT_TYPE_FACE:
      return "Face";
    case NVDS_OBJECT_TYPE_PERSON:
      return "Person";
    case NVDS_OBJECT_TYPE_BAG:
      return "Bag";
    case NVDS_OBJECT_TYPE_BICYCLE:
      return "Bicycle";
    case NVDS_OBJECT_TYPE_ROADSIGN:
      return "RoadSign";
    case NVDS_OBJECT_TYPE_CUSTOM:
      return "Custom";
    case NVDS_OBJECT_TYPE_UNKNOWN:
      return objectId ? objectId : "Unknown";
    default:
      return "Unknown";
  }
}

static const gchar*
to_str (gchar* cstr)
{
    return reinterpret_cast<const gchar*>(cstr) ? cstr : "";
}

static const gchar *
sensor_id_to_str (NvDsMsg2pCtx *ctx, gint sensorId)
{
  NvDsPayloadPriv *privObj = NULL;
  NvDsSensorObject *dsObj = NULL;

  g_return_val_if_fail (ctx, NULL);
  g_return_val_if_fail (ctx->privData, NULL);

  privObj = (NvDsPayloadPriv *) ctx->privData;

  auto idMap = privObj->sensorObj.find (sensorId);
  if (idMap != privObj->sensorObj.end()) {
    dsObj = &idMap->second;
    return dsObj->id.c_str();
  } else {
    cout << "No entry for " CONFIG_GROUP_SENSOR << sensorId
        << " in configuration file" << endl;
    return NULL;
  }
}

static gchar*
generate_deepstream_message_minimal (NvDsMsg2pCtx *ctx, NvDsEvent *events, guint size)
{
  /*
  The JSON structure of the frame
  {
   "version": "4.0",
   "id": "frame-id",
   "@timestamp": "2018-04-11T04:59:59.828Z",
   "sensorId": "sensor-id",
   "objects": [
      ".......object-1 attributes...........",
      ".......object-2 attributes...........",
      ".......object-3 attributes..........."
    ]
  }
  */

  /*
  An example object with Vehicle object-type
  {
    "version": "4.0",
    "id": "frame-id",
    "@timestamp": "2018-04-11T04:59:59.828Z",
    "sensorId": "sensor-id",
    "objects": [
        "957|1834|150|1918|215|Vehicle|#|sedan|Bugatti|M|blue|CA 444|California|0.8",
        "..........."
    ]
  }
   */

  JsonNode *rootNode;
  JsonObject *jobject;
  JsonArray *jArray;
  guint i;
  stringstream ss;
  gchar *message = NULL;

  jArray = json_array_new ();

  for (i = 0; i < size; i++) {

    ss.str("");
    ss.clear();

    NvDsEventMsgMeta *meta = events[i].metadata;
    ss << meta->trackingId << "|" << meta->bbox.left << "|" << meta->bbox.top
        << "|" << meta->bbox.left + meta->bbox.width << "|" << meta->bbox.top + meta->bbox.height
        << "|" << object_enum_to_str (meta->objType, meta->objectId);

    if (meta->extMsg && meta->extMsgSize) {
      // Attach secondary inference attributes.
      switch (meta->objType) {
        case NVDS_OBJECT_TYPE_VEHICLE: {
          NvDsVehicleObject *dsObj = (NvDsVehicleObject *) meta->extMsg;
          if (dsObj) {
            ss << "|#|" << to_str(dsObj->type) << "|" << to_str(dsObj->make) << "|"
               << to_str(dsObj->model) << "|" << to_str(dsObj->color) << "|" << to_str(dsObj->license)
               << "|" << to_str(dsObj->region) << "|" << meta->confidence;
          }
        }
          break;
        case NVDS_OBJECT_TYPE_PERSON: {
          NvDsPersonObject *dsObj = (NvDsPersonObject *) meta->extMsg;
          if (dsObj) {
            ss << "|#|" << to_str(dsObj->gender) << "|" << dsObj->age << "|"
                << to_str(dsObj->hair) << "|" << to_str(dsObj->cap) << "|" << to_str(dsObj->apparel)
                << "|" << meta->confidence;
          }
        }
          break;
        default:
          cout << "Object type (" << meta->objType << ") not implemented" << endl;
          break;
      }
    }

    json_array_add_string_element (jArray, ss.str().c_str());
  }

  // It is assumed that all events / objects are associated with same frame.
  // Therefore ts / sensorId / frameId of first object can be used.

  jobject = json_object_new ();
  json_object_set_string_member (jobject, "version", "4.0");
  json_object_set_int_member (jobject, "id", events[0].metadata->frameId);
  json_object_set_string_member (jobject, "@timestamp", events[0].metadata->ts);
  if (events[0].metadata->sensorStr) {
    json_object_set_string_member (jobject, "sensorId", events[0].metadata->sensorStr);
  } else if (ctx->privData) {
    json_object_set_string_member (jobject, "sensorId",
        to_str((gchar *) sensor_id_to_str (ctx, events[0].metadata->sensorId)));
  } else {
    json_object_set_string_member (jobject, "sensorId", "0");
  }

  json_object_set_array_member (jobject, "objects", jArray);
  rootNode = json_node_new (JSON_NODE_OBJECT);
  json_node_set_object (rootNode, jobject);
  message = json_to_string (rootNode, TRUE);
  json_node_free (rootNode);
  json_object_unref (jobject);

  return message;
}

static bool
nvds_msg2p_parse_sensor (NvDsMsg2pCtx *ctx, GKeyFile *key_file, gchar *group)
{
  bool ret = false;
  bool isEnabled = false;
  gchar **keys = NULL;
  gchar **key = NULL;
  GError *error = NULL;
  NvDsPayloadPriv *privObj = NULL;
  NvDsSensorObject sensorObj;
  gint sensorId;
  gchar *keyVal;

  if (sscanf (group, CONFIG_GROUP_SENSOR "%u", &sensorId) < 1) {
    cout << "Wrong sensor group name " << group << endl;
    return ret;
  }

  privObj = (NvDsPayloadPriv *) ctx->privData;

  auto idMap = privObj->sensorObj.find (sensorId);
  if (idMap != privObj->sensorObj.end()) {
    cout << "Duplicate entries for " << group << endl;
    return ret;
  }

  isEnabled = g_key_file_get_boolean (key_file, group, CONFIG_KEY_ENABLE,
                                      &error);
  if (!isEnabled) {
    // Not enabled, skip the parsing of keys.
    ret = true;
    goto done;
  } else {
    g_key_file_remove_key (key_file, group, CONFIG_KEY_ENABLE,
                           &error);
    CHECK_ERROR (error);
  }

  keys = g_key_file_get_keys (key_file, group, NULL, &error);
  CHECK_ERROR (error);

  for (key = keys; *key; key++) {
    keyVal = NULL;
    if (!g_strcmp0 (*key, CONFIG_KEY_ID)) {
      keyVal = g_key_file_get_string (key_file, group,
                                      CONFIG_KEY_ID, &error);
      sensorObj.id = keyVal;
      CHECK_ERROR (error);
    } else if (!g_strcmp0 (*key, CONFIG_KEY_TYPE)) {
      keyVal = g_key_file_get_string (key_file, group,
                                      CONFIG_KEY_TYPE, &error);
      sensorObj.type = keyVal;
      CHECK_ERROR (error);
    } else if (!g_strcmp0 (*key, CONFIG_KEY_DESCRIPTION)) {
      keyVal = g_key_file_get_string (key_file, group,
                                      CONFIG_KEY_DESCRIPTION, &error);
      sensorObj.desc = keyVal;
      CHECK_ERROR (error);
    } else {
      cout << "Unknown key " << *key << " for group [" << group <<"]\n";
    }

    if (keyVal)
      g_free (keyVal);
  }

  privObj->sensorObj.insert (make_pair (sensorId, sensorObj));

  ret = true;

done:
  if (error) {
    g_error_free (error);
  }
  if (keys) {
    g_strfreev (keys);
  }

  return ret;
}

static bool
nvds_msg2p_parse_csv (NvDsMsg2pCtx *ctx, const gchar *file)
{
  NvDsPayloadPriv *privObj = NULL;
  NvDsSensorObject sensorObj;
  bool retVal = true;
  bool firstRow = true;
  string line;
  gint i, index = 0;

  ifstream inputFile (file);
  if (!inputFile.is_open()) {
    cout << "Couldn't open CSV file " << file << endl;
    return false;
  }

  privObj = (NvDsPayloadPriv *) ctx->privData;

  try {

    while (getline (inputFile, line)) {

      if (firstRow) {
        // Discard first row as it will have header fields.
        firstRow = false;
        continue;
      }

      vector<string> tokens;
      get_csv_tokens (line, tokens);
      // Ignore first cameraId field.
      i = 1;

      // sensor object fields
      sensorObj.id = tokens.at(i++);
      sensorObj.type = "Camera";
      sensorObj.desc = tokens.at(i++);

      privObj->sensorObj.insert (make_pair (index, sensorObj));
      index++;
    }
  } catch (const std::out_of_range& oor) {
    std::cerr << "Out of Range error: " << oor.what() << '\n';
    retVal = false;
  }

  inputFile.close ();
  return retVal;
}

static bool
nvds_msg2p_parse_key_value (NvDsMsg2pCtx *ctx, const gchar *file)
{
  bool retVal = true;
  GKeyFile *cfgFile = NULL;
  GError *error = NULL;
  gchar **groups = NULL;
  gchar **group;

  cfgFile = g_key_file_new ();
  if (!g_key_file_load_from_file (cfgFile, file, G_KEY_FILE_NONE, &error)) {
    g_message ("Failed to load file: %s", error->message);
    retVal = false;
    goto done;
  }

  groups = g_key_file_get_groups (cfgFile, NULL);

  for (group = groups; *group; group++) {
    if (!strncmp (*group, CONFIG_GROUP_SENSOR, strlen (CONFIG_GROUP_SENSOR))) {
      retVal = nvds_msg2p_parse_sensor (ctx, cfgFile, *group);
    } else {
      cout << "Unknown group " << *group << endl;
    }

    if (!retVal) {
      cout << "Failed to parse group " << *group << endl;
      goto done;
    }
  }

done:
  if (groups)
    g_strfreev (groups);

  if (cfgFile)
    g_key_file_free (cfgFile);

  return retVal;
}

NvDsMsg2pCtx* nvds_msg2p_ctx_create (const gchar *file, NvDsPayloadType type)
{
  NvDsMsg2pCtx *ctx = NULL;
  string str;
  bool retVal = true;

  /*
   * Need to parse configuration / CSV files to get static properties of
   * components (e.g. sensor, place etc.) in case of full deepstream schema.
   */
  if (type == NVDS_PAYLOAD_DEEPSTREAM) {
    g_return_val_if_fail (file, NULL);

    ctx = new NvDsMsg2pCtx;
    ctx->privData = (void *) new NvDsPayloadPriv;

    if (g_str_has_suffix (file, ".csv")) {
      retVal = nvds_msg2p_parse_csv (ctx, file);
    } else {
      retVal = nvds_msg2p_parse_key_value (ctx, file);
    }
  } else {
    ctx = new NvDsMsg2pCtx;
    /* If configuration file is provided for minimal schema,
     * parse it for static values.
     */
    if (file) {
      ctx->privData = (void *) new NvDsPayloadPriv;
      retVal = nvds_msg2p_parse_key_value (ctx, file);
    } else {
      ctx->privData = nullptr;
      retVal = true;
    }
  }

  ctx->payloadType = type;

  if (!retVal) {
    cout << "Error in creating instance" << endl;

    if (ctx && ctx->privData)
      delete (NvDsPayloadPriv *) ctx->privData;

    if (ctx) {
      delete ctx;
      ctx = NULL;
    }
  }
  return ctx;
}

void nvds_msg2p_ctx_destroy (NvDsMsg2pCtx *ctx)
{
  delete (NvDsPayloadPriv *) ctx->privData;
  ctx->privData = nullptr;
  delete ctx;
}

NvDsPayload**
nvds_msg2p_generate_multiple (NvDsMsg2pCtx *ctx, NvDsEvent *events, guint eventSize,
                     guint *payloadCount)
{
  gchar *message = NULL;
  gint len = 0;
  NvDsPayload **payloads = NULL;
  *payloadCount = 0;
  //Set how many payloads are being sent back to the plugin
  payloads = (NvDsPayload **) g_malloc0 (sizeof (NvDsPayload*) * 1);

  if (ctx->payloadType == NVDS_PAYLOAD_DEEPSTREAM) {
    message = generate_schema_message (ctx, events->metadata);
    if (message) {
      payloads[*payloadCount]= (NvDsPayload *) g_malloc0 (sizeof (NvDsPayload));
      len = strlen (message);
      // Remove '\0' character at the end of string and just copy the content.
      payloads[*payloadCount]->payload = g_memdup (message, len);
      payloads[*payloadCount]->payloadSize = len;
      ++(*payloadCount);
      g_free (message);
    }
  } else if (ctx->payloadType == NVDS_PAYLOAD_DEEPSTREAM_MINIMAL) {
    message = generate_deepstream_message_minimal (ctx, events, eventSize);
    if (message) {
      len = strlen (message);
      payloads[*payloadCount] = (NvDsPayload *) g_malloc0 (sizeof (NvDsPayload));
      // Remove '\0' character at the end of string and just copy the content.
      payloads[*payloadCount]->payload = g_memdup (message, len);
      payloads[*payloadCount]->payloadSize = len;
      ++(*payloadCount);
      g_free (message);
    }
  } else if (ctx->payloadType == NVDS_PAYLOAD_CUSTOM) {
    payloads[*payloadCount] = (NvDsPayload *) g_malloc0 (sizeof (NvDsPayload));
    payloads[*payloadCount]->payload = (gpointer) g_strdup ("CUSTOM Schema");
    payloads[*payloadCount]->payloadSize = strlen ((char *)payloads[*payloadCount]->payload) + 1;
    ++(*payloadCount);
  } else
    payloads = NULL;

  return payloads;
}

NvDsPayload*
nvds_msg2p_generate (NvDsMsg2pCtx *ctx, NvDsEvent *events, guint size)
{
  gchar *message = NULL;
  gint len = 0;
  NvDsPayload *payload = (NvDsPayload *) g_malloc0 (sizeof (NvDsPayload));

  if (ctx->payloadType == NVDS_PAYLOAD_DEEPSTREAM) {
    message = generate_schema_message (ctx, events->metadata);
    if (message) {
      len = strlen (message);
      // Remove '\0' character at the end of string and just copy the content.
      payload->payload = g_memdup (message, len);
      payload->payloadSize = len;
      g_free (message);
    }
  } else if (ctx->payloadType == NVDS_PAYLOAD_DEEPSTREAM_MINIMAL) {
    message = generate_deepstream_message_minimal (ctx, events, size);
    if (message) {
      len = strlen (message);
      // Remove '\0' character at the end of string and just copy the content.
      payload->payload = g_memdup (message, len);
      payload->payloadSize = len;
      g_free (message);
    }
  } else if (ctx->payloadType == NVDS_PAYLOAD_CUSTOM) {
    payload->payload = (gpointer) g_strdup ("CUSTOM Schema");
    payload->payloadSize = strlen ((char *)payload->payload) + 1;
  } else
    payload->payload = NULL;

  return payload;
}

void
nvds_msg2p_release (NvDsMsg2pCtx *ctx, NvDsPayload *payload)
{
  g_free (payload->payload);
  g_free (payload);
}
