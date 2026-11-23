/* Execute a bound target row with typed SPI parameters. */
#ifndef SYNCHDB_SPI_WRITER_H
#define SYNCHDB_SPI_WRITER_H

#include "executor/target_change.h"
#include "executor/writer_result.h"

/* Caller owns the active transaction, snapshot and SPI connection. */
extern SynchdbRowApplyResult synchdb_spi_writer_apply(const SynchdbTargetChange *change);

#endif /* SYNCHDB_SPI_WRITER_H */
