#ifndef __BH_BEEGFS_IOCTL_H__
#define __BH_BEEGFS_IOCTL_H__

#include <fcntl.h>
#include <string.h>
#include <sys/ioctl.h>
#include <stdint.h>
#ifndef __cplusplus
#include <stdbool.h>
#endif

// entryID string is made of three 32 bit values in hexadecimal form plus two dashes
// (see common/toolkit/StorageTk.h)
#define BEEGFS_IOCTL_ENTRYID_MAXLEN           26
#define BEEGFS_IOCTL_FILENAME_MAXLEN          256
#define BEEGFS_IOCTL_MAX_STRIPE_TARGETS       256
#define BEEGFS_IOCTL_MAX_RST_IDS              256

#define BEEGFS_IOCNUM_GET_STRIPETARGET        26

#define BEEGFS_IOCTYPE_ID                     'f'
#define BEEGFS_IOCTL_NODEALIAS_BUFLEN         256

// stripe pattern types
#define BEEGFS_STRIPEPATTERN_INVALID          0
#define BEEGFS_STRIPEPATTERN_RAID0            1
#define BEEGFS_STRIPEPATTERN_RAID10           2
#define BEEGFS_STRIPEPATTERN_BUDDYMIRROR      3

#define BEEGFS_IOCNUM_GET_STRIPEINFO          25
#define BEEGFS_IOCNUM_GETENTRYINFO            31
#define BEEGFS_IOCNUM_GETENTRYINFO_V2         34

/**
 * Struct for details of a stripe target
 */
struct BeegfsIoctl_GetStripeTargetV2_Arg
{
   /* inputs */
   uint32_t targetIndex;

   /* outputs */
   uint32_t targetOrGroup; // target ID if the file is not buddy mirrored, otherwise mirror group ID

   uint32_t primaryTarget; // target ID != 0 if buddy mirrored
   uint32_t secondaryTarget; // target ID != 0 if buddy mirrored

   uint32_t primaryNodeID; // node ID of target (if unmirrored) or primary target (if mirrored)
   uint32_t secondaryNodeID; // node ID of secondary target, or 0 if unmirrored

   char primaryNodeAlias[BEEGFS_IOCTL_NODEALIAS_BUFLEN];
   char secondaryNodeAlias[BEEGFS_IOCTL_NODEALIAS_BUFLEN];
};


/* used to get the stripe info of a file */
struct BeegfsIoctl_GetStripeInfo_Arg
{
   unsigned outPatternType; // (out-value) stripe pattern type (STRIPEPATTERN_...)
   unsigned outChunkSize; // (out-value) chunksize for striping
   uint16_t outNumTargets; // (out-value) number of stripe targets of given file
};

struct BeegfsIoctl_GetEntryInfo_Arg
{
   uint32_t ownerID;
   char parentEntryID[BEEGFS_IOCTL_ENTRYID_MAXLEN + 1];
   char entryID[BEEGFS_IOCTL_ENTRYID_MAXLEN + 1];
   int entryType;
   int featureFlags;
};

#define BEEGFS_IOC_GET_STRIPEINFO          _IOR( \
   BEEGFS_IOCTYPE_ID, BEEGFS_IOCNUM_GET_STRIPEINFO, struct BeegfsIoctl_GetStripeInfo_Arg)
#define BEEGFS_IOC_GETENTRYINFO             _IOR( \
   BEEGFS_IOCTYPE_ID, BEEGFS_IOCNUM_GETENTRYINFO, struct BeegfsIoctl_GetEntryInfo_Arg)
#define BEEGFS_IOC_GET_STRIPETARGET_V2     _IOR( \
   BEEGFS_IOCTYPE_ID, BEEGFS_IOCNUM_GET_STRIPETARGET, struct BeegfsIoctl_GetStripeTargetV2_Arg)
#define BEEGFS_IOC_GETENTRYINFO_V2         _IOWR( \
   BEEGFS_IOCTYPE_ID, BEEGFS_IOCNUM_GETENTRYINFO_V2, struct BeegfsIoctl_GetEntryInfoV2_Arg)



/*
 * V2 entry info: returns comprehensive entry information including stripe pattern
 * and all stripe target IDs in a single ioctl call to the meta server.
 *
 * The fd MUST be an open directory.  If filename is non-empty it names a direct
 * child of that directory; if empty ("") the ioctl queries the directory fd itself.
 * stripeTargetIDs[] holds target IDs for RAID0/RAID10 or mirror group IDs for
 * BUDDYMIRROR; check patternType to distinguish.  Only the first numTargets
 * entries are valid (capped at BEEGFS_IOCTL_MAX_STRIPE_TARGETS).
 */
struct BeegfsIoctl_GetEntryInfoV2_Arg
{
   char filename[BEEGFS_IOCTL_FILENAME_MAXLEN];  /* in/out: child name or "" for dir fd itself */

   /* Basic entry info (same fields as V1) */
   uint32_t ownerID;
   char parentEntryID[BEEGFS_IOCTL_ENTRYID_MAXLEN + 1];
   char entryID[BEEGFS_IOCTL_ENTRYID_MAXLEN + 1];
   int entryType;
   int featureFlags;

   /* Stripe pattern */
   uint32_t patternType;
   uint32_t chunkSize;
   uint32_t storagePoolId;
   uint32_t defaultNumTargets;
   uint16_t numTargets;
   uint16_t stripeTargetIDs[BEEGFS_IOCTL_MAX_STRIPE_TARGETS];

   /* PathInfo */
   uint32_t pathInfoFlags;
   uint32_t origParentUID;
   char origParentEntryID[BEEGFS_IOCTL_ENTRYID_MAXLEN + 1];

   /* Remote Storage Target (RST) */
   uint8_t  rstMajorVersion;
   uint8_t  rstMinorVersion;
   uint16_t rstCoolDownPeriod;
   uint16_t rstFilePolicies;
   uint32_t numRSTIds;
   uint32_t rstIds[BEEGFS_IOCTL_MAX_RST_IDS];

   /* Session and state info */
   uint32_t numSessionsRead;
   uint32_t numSessionsWrite;
   uint8_t  fileDataState;
   /* Zero (FhgfsOpsErr_SUCCESS) means full data is populated. Non-zero means
    * partial result: only basic entry info fields above are valid; stripe
    * pattern, PathInfo, RST, and session fields are left unset. Non-zero
    * values are FhgfsOpsErr codes. */
   int32_t  getEntryInfoResult;
};

static inline bool beegfs_getEntryInfo(int fd, uint32_t* ownerID, char* parentEntryID,
      char* entryID, int* entryType, int* featureFlags);
static inline bool beegfs_getStripeInfo(int fd, unsigned* outPatternType, unsigned* outChunkSize,
   uint16_t* outNumTargets);
static inline bool beegfs_getStripeTargetV2(int fd, uint32_t targetIndex,
   struct BeegfsIoctl_GetStripeTargetV2_Arg* outTargetInfo);
static inline bool beegfs_getEntryInfoV2(int dirfd, const char* filename,
   struct BeegfsIoctl_GetEntryInfoV2_Arg* out);

/**
 * Get entryInfo data for given file.
 *
 * @param fd filedescriptor pointing to some file inside a BeeGFS mount.
 * @param ownerID pointer to an uint32_t in which the ownerID shall be stored
 * @param parentEntryID pointer to a buffer for the parent entryID. The buffer must
 * be at least BEEGFS_IOCTL_ENTRYID_MAXLEN + 1 bytes long.
 * @param entryID pointer to a buffer for the entryID. The buffer must
 * be at least BEEGFS_IOCTL_ENTRYID_MAXLEN + 1 bytes long.
 * @param entryType pointer to an int in which the entryType shall be stored
 * @param featureFlags pointer to an int in which the feature flags shall be stored
 * @return success/failure
 */
static inline bool beegfs_getEntryInfo(int fd, uint32_t* ownerID, char* parentEntryID,
      char* entryID, int* entryType, int* featureFlags)
{
   struct BeegfsIoctl_GetEntryInfo_Arg arg;

   memset(&arg, 0, sizeof(arg));

   if(ioctl(fd, BEEGFS_IOC_GETENTRYINFO, &arg))
   {
      return false;
   }

   *ownerID = arg.ownerID;
   strncpy(parentEntryID, arg.parentEntryID, BEEGFS_IOCTL_ENTRYID_MAXLEN + 1);
   strncpy(entryID, arg.entryID, BEEGFS_IOCTL_ENTRYID_MAXLEN + 1);
   *entryType = arg.entryType;
   *featureFlags = arg.featureFlags;

   return true;
}

/**
 * Get the stripe info of a file.
 *
 * @param fd filedescriptor pointing to some file inside a BeeGFS mount.
 * @param outPatternType type of stripe pattern (BEEGFS_STRIPEPATTERN_...)
 * @param outChunkSize chunk size for striping.
 * @param outNumTargets number of targets for striping.
 * @return true on success, false on error (in which case errno will be set).
 */
static inline bool beegfs_getStripeInfo(int fd, unsigned* outPatternType, unsigned* outChunkSize,
   uint16_t* outNumTargets)
{
   struct BeegfsIoctl_GetStripeInfo_Arg getStripeInfo;

   int res = ioctl(fd, BEEGFS_IOC_GET_STRIPEINFO, &getStripeInfo);
   if(res)
      return false;

   *outPatternType = getStripeInfo.outPatternType;
   *outChunkSize = getStripeInfo.outChunkSize;
   *outNumTargets = getStripeInfo.outNumTargets;

   return true;
}


/**
 * Get the stripe target of a file (with 0-based index).
 *
 * @param fd filedescriptor pointing to some file inside a BeeGFS mount.
 * @param targetIndex index of target that should be retrieved (start with 0 and then call this
 *        again with index up to "*outNumTargets-1" to retrieve remaining targets).
 * @param outTargetInfo pointer to struct that will be filled with information about the selected
 *        stripe target
 * @return true on success, false on error (in which case errno will be set).
 */
static inline bool beegfs_getStripeTargetV2(int fd, uint32_t targetIndex,
   struct BeegfsIoctl_GetStripeTargetV2_Arg* outTargetInfo)
{
   memset(outTargetInfo, 0, sizeof(*outTargetInfo));

   outTargetInfo->targetIndex = targetIndex;

   return ioctl(fd, BEEGFS_IOC_GET_STRIPETARGET_V2, outTargetInfo) == 0;
}

/**
 * Get comprehensive entry info for a BeeGFS file or directory in one ioctl.
 *
 * @param dirfd  Open directory fd (must be a directory, not the file itself).
 * @param filename  Name of a direct child to query, or "" to query dirfd itself.
 *                  Must not contain '/' and must fit in BEEGFS_IOCTL_FILENAME_MAXLEN.
 * @param out    Pointer to struct that will be filled with entry and stripe info.
 * @return true on success, false on error (errno will be set).
 */
static inline bool beegfs_getEntryInfoV2(int dirfd, const char* filename,
   struct BeegfsIoctl_GetEntryInfoV2_Arg* out)
{
   memset(out, 0, sizeof(*out));

   if (filename) {
      strncpy(out->filename, filename, BEEGFS_IOCTL_FILENAME_MAXLEN - 1);
   }

   return ioctl(dirfd, BEEGFS_IOC_GETENTRYINFO_V2, out) == 0;
}


#endif /* __BH_BEEGFS_IOCTL_H__ */

