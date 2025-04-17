package model

import "github.com/multiformats/go-multicodec"

type Provider struct {
	Name          string
	Probabilities map[multicodec.Code]Probabilities
}

type Population struct {
	Providers []PC
}

type PC struct {
	Provider Provider
	Count    int
}

func (p *Population) Add(provider Provider, count int) {
	if p.Providers == nil {
		p.Providers = make([]PC, 0)
	}
	p.Providers = append(p.Providers, PC{Provider: provider, Count: count})
}
