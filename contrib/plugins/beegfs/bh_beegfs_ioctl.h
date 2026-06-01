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

#define BEEGFS_IOCTYPE_ID                     'f'

// stripe pattern types
#define BEEGFS_STRIPEPATTERN_INVALID          0
#define BEEGFS_STRIPEPATTERN_RAID0            1
#define BEEGFS_STRIPEPATTERN_RAID10           2
#define BEEGFS_STRIPEPATTERN_BUDDYMIRROR      3

#define BEEGFS_IOCNUM_GETENTRYINFO_V2         34

#define BEEGFS_IOC_GETENTRYINFO_V2         _IOWR( \
   BEEGFS_IOCTYPE_ID, BEEGFS_IOCNUM_GETENTRYINFO_V2, struct BeegfsIoctl_GetEntryInfoV2_Arg)

/*
 * Comprehensive entry info (basic + stripe + PathInfo + RST) in one ioctl to the
 * meta server. fd must be an open directory; filename names a direct child or ""
 * for the directory itself. stripeTargetIDs[] holds target IDs (RAID0/RAID10) or
 * mirror group IDs (BUDDYMIRROR) per patternType; only the first numTargets valid.
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
   /* 0 (FhgfsOpsErr_SUCCESS): full result. Non-zero (FhgfsOpsErr code): partial,
    * only basic entry fields valid; stripe/PathInfo/RST/session left unset. */
   int32_t  getEntryInfoResult;
};

/* dirfd: open directory. filename: direct child, or "" for the dir itself.
 * Returns true on success, false on error (errno set). */
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

