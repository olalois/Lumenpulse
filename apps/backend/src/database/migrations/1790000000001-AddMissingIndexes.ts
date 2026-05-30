import { MigrationInterface, QueryRunner } from 'typeorm';

export class AddMissingIndexes1790000000001 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {}
  public async down(queryRunner: QueryRunner): Promise<void> {}
}