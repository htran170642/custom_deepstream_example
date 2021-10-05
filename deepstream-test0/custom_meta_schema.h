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

/**
 * @file
 * <b>NVIDIA DeepStream: Metadata Extension Structures</b>
 *
 * @b Description: This file defines the NVIDIA DeepStream metadata structures
 * used to describe metadata objects.
 */

/**
 * @defgroup  metadata_extensions  Metadata Extension Structures
 *
 * Defines metadata structures used to describe metadata objects.
 *
 * @ingroup NvDsMetaApi
 * @{
 */

#ifndef NVDSCUSTOMMETA_H_
#define NVDSCUSTOMMETA_H_

#include "nvdsmeta_schema.h"
#include <glib.h>

#ifdef __cplusplus
extern "C"
{
#endif

#define PERSON_LABEL "person"
#define FACE_LABEL "face"
#define MAX_TIME_STAMP_LEN 32
#define MAX_OBJ_NUM 256

#define PET_MODULE_NAME "pet"


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

}NvDsSimpleObjectMetaList;


typedef struct
{
  gchar* sourceUri;
  gint sourceId;
  gint sourceType;
  guint frameId;

  guint frameWidth;
  guint frameHeight;

  guint objCounts;
  NvDsSimpleObjectMetaList objMetaList[MAX_OBJ_NUM];

  gchar* filterCloudModules;
  gchar* sourceCloudModules;
}NvDsFrameObjDescEvent;

#ifdef __cplusplus
}
#endif
#endif /* NVDSMETA_H_ */

/** @} */
