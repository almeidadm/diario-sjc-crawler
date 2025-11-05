"""Parser para metadados JSON das edições (Fase 1)."""

import json
import logging
from typing import Any

import httpx

from ..models import GazetteMetadata

logger = logging.getLogger(__name__)


class MetadataParser:
    """Parseia JSON de metadados das edições do diário."""

    @staticmethod
    def parse(response: httpx.Response) -> list[GazetteMetadata]:
        """
        Extrai metadados de edições do JSON da API.
        
        Args:
            response: Resposta HTTP com JSON de metadados
            
        Returns:
            Lista de GazetteMetadata extraídos
        """
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            logger.warning(f"JSON inválido em {response.url}: {e}")
            return []

        # Valida estrutura do JSON
        if not data or data.get("erro") or not data.get("itens"):
            logger.warning(f"JSON vazio ou com erro em {response.url}")
            return []

        publication_date = data.get("data", "unknown")
        items = data.get("itens", [])

        metadata_list = []
        for item in items:
            try:
                edition_id = str(item.get("id"))
                
                # CORREÇÃO: Garantir que supplement seja booleano
                supplement = item.get("suplemento")
                if supplement is None:
                    supplement_bool = False
                elif isinstance(supplement, bool):
                    supplement_bool = supplement
                elif isinstance(supplement, str):
                    supplement_bool = supplement.lower() in ('true', '1', 'yes')
                elif isinstance(supplement, int):
                    supplement_bool = bool(supplement)
                else:
                    supplement_bool = False
                
                metadata = GazetteMetadata(
                    edition_id=edition_id,
                    publication_date=publication_date,
                    edition_number=int(item.get("numero", 0)),
                    supplement=supplement_bool,  # CORREÇÃO: Agora é sempre booleano
                    edition_type_id=int(item.get("tipo_edicao_id", 0)),
                    edition_type_name=str(item.get("tipo_edicao_nome", "")),
                    pdf_url=f"https://diariodomunicipio.sjc.sp.gov.br/apifront/portal/edicoes/pdf_diario/{edition_id}/",
                )
                metadata_list.append(metadata)
                
            except Exception as e:
                logger.error(f"Erro ao processar item de metadado: {e}")
                continue

        logger.debug(f"Extraídos {len(metadata_list)} metadados de {response.url}")
        return metadata_list