package br.com.alura.domain.audit;

public class Audit {

    private final Long id;
    private final String cnpj;
    private final String situacaoCadastral;

    public Audit(Long id, String cnpj, String situacaoCadastral) {
        this.id = id;
        this.cnpj = cnpj;
        this.situacaoCadastral = situacaoCadastral;
    }

    public Long getId() {
        return id;
    }

    public String getCnpj() {
        return cnpj;
    }

    public String getSituacaoCadastral() {
        return situacaoCadastral;
    }
}
