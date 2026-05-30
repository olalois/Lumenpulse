import { Injectable, Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { IngestSorobanEventDto } from './dto/ingest-soroban-event.dto';
import { ProjectRegistryEntity } from '../database/entities/project-registry.entity';

export const SOROBAN_EVENTS_QUEUE = 'soroban-events';
export const PROCESS_EVENT_JOB = 'process-event';

@Injectable()
export class SorobanEventsService {
  private readonly logger = new Logger(SorobanEventsService.name);

  constructor(
    @InjectQueue(SOROBAN_EVENTS_QUEUE) private readonly queue: Queue,
    
    @InjectRepository(ProjectRegistryEntity)
    private readonly projectRepo: Repository<ProjectRegistryEntity>,
  ) {}

  async ingest(dto: IngestSorobanEventDto): Promise<{ queued: boolean }> {
    const jobId = `${dto.txHash}:${dto.eventIndex}`;

    
    await this.queue.add(PROCESS_EVENT_JOB, dto, {
      jobId,
      attempts: 3,
      backoff: { type: 'exponential', delay: 1000 },
      removeOnComplete: { count: 500 },
      removeOnFail: { count: 200 },
    });

    this.logger.debug(`Queued soroban event ${jobId}`);
    return { queued: true };
  }

  
  async syncProjectRegistryEvent(eventData: any): Promise<void> {
    const { projectId, owner, name, metadataCid, ledgerSeq, txHash } = eventData;

    const existing = await this.projectRepo.findOne({ where: { projectId } });

    
    if (existing && existing.lastLedgerSeq > ledgerSeq) {
      this.logger.debug(`Skipping stale event for Project ${projectId}`);
      return;
    }

    await this.projectRepo.upsert(
      {
        projectId,
        owner,
        name,
        metadataCid: metadataCid ?? existing?.metadataCid,
        lastLedgerSeq: ledgerSeq, // Traceability pointer
        lastTxHash: txHash,       // Traceability pointer
      },
      ['projectId'], // Conflict target prevents duplicate rows
    );
  }
}